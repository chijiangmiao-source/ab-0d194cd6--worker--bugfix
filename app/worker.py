"""Background worker: processes exports only while holding a valid lease.

Loop: recover stuck exports (lease expired/absent) -> process one RECEIVED
export. Processing stages the artifact to a temp file, records and verifies
its digest, then atomically publishes. Fault-injection hooks (TEST_HOOKS)
simulate a crash after a partial write, after staging, or a deterministic
pause before the first temp write (lease-handover regression).
"""
import hashlib
import os
import socket
import sys
import time
import uuid

from . import artifacts, config, recovery, store
from .render import render_artifact_bytes

# Test-only interleaving seams: name -> callable(me, export_id). Empty in
# production; the deterministic regression installs blocking callbacks here.
HOOKS = {}


def install_hook(name, fn):
    HOOKS[name] = fn


def reset_hooks():
    HOOKS.clear()


def identity():
    return "worker-%s-%d-%s" % (socket.gethostname(), os.getpid(), uuid.uuid4().hex[:6])


def lease_resource(export_id):
    return "export:" + export_id


def _crash(me, export_id, mode):
    print("[%s] fault injection: %s on %s -> exiting" % (me, mode, export_id), flush=True)
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(3)


class _LeaseLost(Exception):
    """Raised inside the staging transaction when fencing fails there.

    Propagating it rolls the whole IMMEDIATE transaction back, so a stale
    owner can never leave a recorded staged artifact behind.
    """


def _hook(name, me, export_id):
    """Deterministic interleaving seam for tests; a no-op in production.

    The regression suite blocks a specific worker here to reproduce the
    lease-handover interleaving without wall-clock races.
    """
    fn = HOOKS.get(name)
    if fn is not None:
        fn(me, export_id)


def _wait_lease_superseded(conn, resource, me, fencing):
    """Pause until the lease (or the export itself) has clearly changed hands.

    Used by the pause_before_tmp_write fault so a takeover happens while this
    worker is frozen. This worker is frozen in PROCESSING and never touches
    the stage meanwhile, so any of these proves a handover: another owner, a
    newer fencing token, a missing row (a new owner already released), or the
    stage moving off PROCESSING (recovery requeue/reprocess). Bounded by a
    timeout; the fencing gates after the pause stay authoritative either way.
    """
    deadline = time.time() + config.fault_pause_timeout_seconds()
    while time.time() < deadline:
        row = store.get_lease(conn, resource)
        export = store.get_export(conn, resource.split(":", 1)[1])
        if (
            not row
            or row["owner"] != me
            or row["fencing"] != fencing
            or (export is not None and export["stage"] != "PROCESSING")
        ):
            return True
        time.sleep(0.05)
    return False


def _give_up_lease(conn, export_id, me, tmp, where):
    """A fenced-out worker stops touching the export. It may remove only its
    OWN still-unregistered temp file; anything already registered is handled
    by the current owner's recovery, so it must never be touched here."""
    if tmp is not None:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        except OSError:
            pass
    with store.immediate(conn):
        store.journal(conn, export_id, me, "lease_lost", where)
    return "lease_lost"


def process_export(conn, export_id, me, fencing):
    resource = lease_resource(export_id)
    with store.immediate(conn):
        if not store.cas_stage(conn, export_id, "PROCESSING", ("RECEIVED",)):
            return "skipped"
        store.journal(conn, export_id, me, "processing_started", None)

    export = store.get_export(conn, export_id)
    data = render_artifact_bytes(export)
    digest = hashlib.sha256(data).hexdigest()
    tmp = artifacts.tmp_path(export_id, uuid.uuid4().hex[:8])

    # Fault: die in the middle of the temp write (leaves a partial artifact).
    if store.pop_fault(conn, export_id, "crash_partial_write"):
        artifacts.write_tmp(tmp, data[: max(1, len(data) // 2)])
        with store.immediate(conn):
            store.journal(conn, export_id, me, "fault_partial_write", tmp)
        _crash(me, export_id, "crash_partial_write")

    # Test seam: deterministically stall THIS worker before the first temp
    # write (and before re-checking the lease), so the lease can expire and a
    # takeover worker can recover and publish while the old owner is frozen.
    # The in-process hook drives the unit regression; the one-shot fault drives
    # the multi-container acceptance run.
    _hook("before_tmp_write_gate", me, export_id)
    if store.pop_fault(conn, export_id, "pause_before_tmp_write"):
        with store.immediate(conn):
            store.journal(conn, export_id, me, "fault_pause_before_tmp_write", tmp)
        # Stay frozen until the lease has actually been handed over (or the
        # bounded wait elapses); do not perform any temp write meanwhile.
        _wait_lease_superseded(conn, resource, me, fencing)

    # Fencing revalidation #1: a superseded worker must not write any temp
    # file for an export it no longer owns.
    if not store.check_lease(conn, resource, me, fencing):
        return _give_up_lease(conn, export_id, me, None, "before_tmp_write")

    _hook("before_tmp_write", me, export_id)
    artifacts.write_tmp(tmp, data)
    _hook("after_tmp_write", me, export_id)

    # Fencing revalidation #2: the whole staging group (artifact record +
    # stage CAS + journal) is conditional on still holding the lease, so a
    # stale owner can neither register a staged artifact nor move the stage.
    try:
        with store.immediate(conn):
            if not store.check_lease(conn, resource, me, fencing):
                raise _LeaseLost
            store.record_artifact(conn, export_id, "staged", tmp, digest)
            if not store.cas_stage(conn, export_id, "STAGED", ("PROCESSING",)):
                # The current owner already requeued/reprocessed the export.
                raise _LeaseLost
            store.journal(conn, export_id, me, "staged", "digest=%s path=%s" % (digest, tmp))
    except _LeaseLost:
        return _give_up_lease(conn, export_id, me, tmp, "before_stage_record")

    # Fault: die after the staged artifact + digest are durably recorded.
    if store.pop_fault(conn, export_id, "crash_after_staged"):
        with store.immediate(conn):
            store.journal(conn, export_id, me, "fault_exit_after_staged", tmp)
        _crash(me, export_id, "crash_after_staged")

    _hook("after_staged", me, export_id)

    # Fencing revalidation #3: do not read, verify or publish after losing
    # the lease. Staged residue (if any) belongs to the current owner's
    # recovery, which converges or cleans it.
    if not store.check_lease(conn, resource, me, fencing):
        with store.immediate(conn):
            store.journal(conn, export_id, me, "lease_lost", "after_staged %s" % tmp)
        return "lease_lost"

    # Verify the staged bytes before anything becomes downloadable.
    if artifacts.sha256_file(tmp) != digest:
        artifacts.quarantine(tmp)
        with store.immediate(conn):
            store.journal(conn, export_id, me, "verify_failed", tmp)
            store.requeue(conn, export_id, me, "digest mismatch after staging")
        return "verify_failed"

    # Fencing: only the valid lease holder may publish.
    if not store.check_lease(conn, lease_resource(export_id), me, fencing):
        with store.immediate(conn):
            store.journal(conn, export_id, me, "lease_lost", None)
        return "lease_lost"

    via = artifacts.publish(tmp, artifacts.published_path(export_id), digest)
    with store.immediate(conn):
        store.record_artifact(conn, export_id, "published", artifacts.published_path(export_id), digest)
        published = store.mark_published(conn, export_id, digest, artifacts.published_path(export_id), me, via)
    if published:
        # Final net: after PUBLISHED no staged record may still point at a
        # live temp file, and no temp file may remain for the export.
        recovery.settle_published(conn, export_id, me)
    return "published"


def tick(conn, me):
    did_work = False
    # Recover exports whose owner vanished (lease expired or absent).
    for row in store.stuck_exports(conn):
        export_id = row["export_id"]
        fencing = store.acquire_lease(conn, lease_resource(export_id), me, config.lease_ttl())
        if fencing is None:
            continue
        try:
            recovery.recover_export(conn, export_id, me)
            did_work = True
        finally:
            store.release_lease(conn, lease_resource(export_id), me, fencing)
    # Process one pending export.
    row = store.next_received(conn)
    if row:
        export_id = row["export_id"]
        fencing = store.acquire_lease(conn, lease_resource(export_id), me, config.lease_ttl())
        if fencing is not None:
            try:
                process_export(conn, export_id, me, fencing)
                did_work = True
            finally:
                store.release_lease(conn, lease_resource(export_id), me, fencing)
    return did_work


def run_forever(me=None):
    me = me or identity()
    config.ensure_dirs()
    conn = store.connect()
    store.init_db(conn)
    recovery.sweep_orphans(conn, me)
    print("[%s] worker started (poll=%.2fs lease_ttl=%.1fs)" % (me, config.poll_interval(), config.lease_ttl()), flush=True)
    while True:
        try:
            tick(conn, me)
        except Exception as exc:  # keep the loop alive; next tick retries
            print("[%s] tick error: %r" % (me, exc), file=sys.stderr, flush=True)
            try:
                conn.rollback()
            except Exception:
                pass
        time.sleep(config.poll_interval())


def main():
    run_forever()


if __name__ == "__main__":
    main()
