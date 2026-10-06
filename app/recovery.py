"""Crash recovery.

Runs under the export's lease (worker startup and every tick). For each
unfinished export, consult the journal/artifact records plus on-disk digests:

* the staged temp artifact is complete (digest matches the recorded and the
  deterministically recomputed digest) -> converge: publish that very artifact;
* a published file already exists with the expected digest (crash between the
  atomic link and the DB update) -> converge the bookkeeping;
* anything else (partial write, digest mismatch, missing file, orphans) ->
  clean up the残缺 artifacts and requeue the export.

After convergence the export is settled: PUBLISHED is terminal, so any staged
artifact records a superseded worker may have left behind are aborted and all
temp files are removed -- no staged record may ever reference a live temp file
once an export is published.
"""
import hashlib
import os

from . import artifacts, store
from .render import render_artifact_bytes


def _expected(export_row):
    data = render_artifact_bytes(export_row)
    return data, hashlib.sha256(data).hexdigest()


def _converge(conn, export_id, digest, actor, via):
    with store.immediate(conn):
        store.record_artifact(conn, export_id, "published", artifacts.published_path(export_id), digest)
        store.mark_published(conn, export_id, digest, artifacts.published_path(export_id), actor, via)


def settle_published(conn, export_id, actor):
    """Terminal-state cleanup for a PUBLISHED export.

    Aborts every leftover 'staged' artifact row and removes every temp file of
    the export. Safe to call from any path because it only acts when the export
    has reached the terminal PUBLISHED stage, so it can never disturb another
    worker's in-flight staging. Returns a small {'aborted', 'removed'} report.
    """
    export = store.get_export(conn, export_id)
    if not export or export["stage"] != "PUBLISHED":
        return {"aborted": 0, "removed": []}
    removed = artifacts.cleanup_tmp_for(export_id)
    with store.immediate(conn):
        rows = store.staged_artifacts(conn, export_id)
        for row in rows:
            store.abort_artifact(conn, row["id"])
        if rows or removed:
            store.journal(
                conn, export_id, actor, "published_residue_settled",
                "aborted_staged=%d removed_tmp=%d" % (len(rows), len(removed)),
            )
    return {"aborted": len(rows), "removed": removed}


def recover_export(conn, export_id, actor):
    """Recover one export. Caller must hold the export's lease."""
    export = store.get_export(conn, export_id)
    if not export or export["stage"] == "PUBLISHED":
        return "none"
    _, expected_digest = _expected(export)
    pub = artifacts.published_path(export_id)

    # Case 1: published file already on disk (crash between link and DB update).
    if os.path.exists(pub):
        if artifacts.sha256_file(pub) == expected_digest:
            _converge(conn, export_id, expected_digest, actor, "recovery_published_file")
            settle_published(conn, export_id, actor)
            return "converged"
        target = artifacts.quarantine(pub)
        with store.immediate(conn):
            store.journal(conn, export_id, actor, "recovery_quarantined_published", target)

    # Case 2: a staged temp artifact whose digest matches journal + recompute.
    for row in store.staged_artifacts(conn, export_id):
        path = row["path"]
        if (
            os.path.exists(path)
            and artifacts.sha256_file(path) == row["digest"] == expected_digest
        ):
            artifacts.publish(path, pub, row["digest"])
            _converge(conn, export_id, row["digest"], actor, "recovery_staged_artifact")
            # Abort stale staged rows (e.g. a superseded worker's later record)
            # and sweep all temp files; PUBLISHED must leave no reference.
            settle_published(conn, export_id, actor)
            return "converged"

    # Case 3: incomplete/mismatched remains -> clean up and requeue.
    removed = artifacts.cleanup_tmp_for(export_id)
    with store.immediate(conn):
        for row in store.staged_artifacts(conn, export_id):
            store.abort_artifact(conn, row["id"])
        store.journal(conn, export_id, actor, "recovery_cleanup", "removed=%d" % len(removed))
        store.requeue(conn, export_id, actor, "recovery_cleanup removed=%d" % len(removed))
    return "requeued"


def sweep_orphans(conn, actor, older_than_seconds=30.0):
    """Delete temp files not referenced by any staged artifact record."""
    import time

    removed = []
    known = set()
    for export in store.list_exports(conn, limit=1000):
        for row in store.staged_artifacts(conn, export["export_id"]):
            known.add(os.path.abspath(row["path"]))
    now = time.time()
    for path in artifacts.list_tmp_files():
        if os.path.abspath(path) in known:
            continue
        if now - os.path.getmtime(path) < older_than_seconds:
            continue  # may belong to an in-flight staging; leave it alone
        try:
            os.unlink(path)
            removed.append(path)
        except FileNotFoundError:
            pass
    if removed:
        with store.immediate(conn):
            store.journal(conn, None, actor, "recovery_orphan_sweep", "removed=%d" % len(removed))
    return removed
