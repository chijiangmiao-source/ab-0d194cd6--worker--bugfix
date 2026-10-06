"""Background worker: processes exports only while holding a valid lease.

Loop: recover stuck exports (lease expired/absent) -> process one RECEIVED
export. Processing stages the artifact to a temp file, records and verifies
its digest, then atomically publishes. The lease (fencing token) is
re-checked before every mutation — temp write, staged registration, publish —
so a worker whose lease was taken over never writes or registers artifacts.
Fault-injection hooks (TEST_HOOKS) simulate a crash after a partial write or
after staging.
"""
import hashlib
import os
import socket
import sys
import time
import uuid

from . import artifacts, config, recovery, store
from .render import render_artifact_bytes

# Test-only seams (must stay None in production): pause points around the
# first temp write, called as hook(export_id, worker_identity), so tests can
# reproduce lease-handover interleavings deterministically.
before_tmp_write_hook = None
after_tmp_write_hook = None


def identity():
    return "worker-%s-%d-%s" % (socket.gethostname(), os.getpid(), uuid.uuid4().hex[:6])


def lease_resource(export_id):
    return "export:" + export_id


def _crash(me, export_id, mode):
    print("[%s] fault injection: %s on %s -> exiting" % (me, mode, export_id), flush=True)
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(3)


def process_export(conn, export_id, me, fencing):
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

    if before_tmp_write_hook is not None:
        before_tmp_write_hook(export_id, me)

    # Fencing: a worker whose lease was taken over must not write anything.
    if not store.check_lease(conn, lease_resource(export_id), me, fencing):
        with store.immediate(conn):
            store.journal(conn, export_id, me, "lease_lost", None)
        return "lease_lost"

    artifacts.write_tmp(tmp, data)

    if after_tmp_write_hook is not None:
        after_tmp_write_hook(export_id, me)

    # Register the staged artifact only while still holding the lease: the
    # fencing check and the registration commit in one transaction, so a
    # stale worker can never leave a staged record behind.
    with store.immediate(conn):
        lease_held = store.check_lease(conn, lease_resource(export_id), me, fencing)
        if lease_held:
            store.record_artifact(conn, export_id, "staged", tmp, digest)
            store.cas_stage(conn, export_id, "STAGED", ("PROCESSING",))
            store.journal(conn, export_id, me, "staged", "digest=%s path=%s" % (digest, tmp))
    if not lease_held:
        artifacts.discard_tmp(tmp)
        with store.immediate(conn):
            store.journal(conn, export_id, me, "lease_lost", None)
        return "lease_lost"

    # Fault: die after the staged artifact + digest are durably recorded.
    if store.pop_fault(conn, export_id, "crash_after_staged"):
        with store.immediate(conn):
            store.journal(conn, export_id, me, "fault_exit_after_staged", tmp)
        _crash(me, export_id, "crash_after_staged")

    # Verify the staged bytes before anything becomes downloadable.
    if artifacts.sha256_file(tmp) != digest:
        artifacts.quarantine(tmp)
        with store.immediate(conn):
            store.journal(conn, export_id, me, "verify_failed", tmp)
            store.requeue(conn, export_id, me, "digest mismatch after staging")
        return "verify_failed"

    # Fencing: only the valid lease holder may publish. A stale worker drops
    # the temp file and the staged record it registered itself, so nothing
    # it staged stays referenceable after the takeover.
    if not store.check_lease(conn, lease_resource(export_id), me, fencing):
        artifacts.discard_tmp(tmp)
        with store.immediate(conn):
            store.abort_staged_by_path(conn, export_id, tmp)
            store.journal(conn, export_id, me, "lease_lost", None)
        return "lease_lost"

    via = artifacts.publish(tmp, artifacts.published_path(export_id), digest)
    with store.immediate(conn):
        store.record_artifact(conn, export_id, "published", artifacts.published_path(export_id), digest)
        store.mark_published(conn, export_id, digest, artifacts.published_path(export_id), me, via)
        # PUBLISHED is terminal: staged records are superseded and must not
        # stay referenceable.
        store.abort_staged(conn, export_id)
    artifacts.cleanup_tmp_for(export_id)
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
