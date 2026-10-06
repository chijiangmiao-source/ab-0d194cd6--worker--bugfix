import hashlib
import os
import tempfile
import threading
import time
import unittest

from app import artifacts, config, recovery, store, worker
from app.render import render_artifact_bytes


class WorkerTestBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        os.environ["DATA_DIR"] = self._tmp.name
        os.environ["LEASE_TTL_SECONDS"] = "5"
        config.ensure_dirs()
        self.conn = store.connect()
        store.init_db(self.conn)

    def tearDown(self):
        worker.reset_hooks()
        self.conn.close()
        self._tmp.cleanup()
        for var in ("DATA_DIR", "LEASE_TTL_SECONDS"):
            os.environ.pop(var, None)


class ProcessTest(WorkerTestBase):
    def test_process_publishes_verified_artifact(self):
        store.submit_export(self.conn, "E-1", [{"ts": "t0", "lat": 31.2, "depth_m": 10}])
        fencing = store.acquire_lease(self.conn, worker.lease_resource("E-1"), "w-test", 5)
        result = worker.process_export(self.conn, "E-1", "w-test", fencing)
        self.assertEqual("published", result)
        row = store.get_export(self.conn, "E-1")
        self.assertEqual("PUBLISHED", row["stage"])
        data = artifacts.load_verified(row)  # digest verified
        self.assertEqual(hashlib.sha256(data).hexdigest(), row["artifact_digest"])
        self.assertEqual([], artifacts.tmp_files_for("E-1"))

    def test_two_workers_publish_exactly_once(self):
        """Two racing worker loops: one export, one published artifact, no regression."""
        store.submit_export(self.conn, "E-1", [{"ts": "t0", "lat": 31.2, "depth_m": 10}])
        stop = threading.Event()

        def loop(name):
            conn = store.connect()
            try:
                while not stop.is_set():
                    try:
                        worker.tick(conn, name)
                    except Exception:
                        conn.rollback()
                    time.sleep(0.02)
            finally:
                conn.close()

        threads = [threading.Thread(target=loop, args=("w-%d" % i,)) for i in range(2)]
        for t in threads:
            t.start()
        deadline = time.time() + 15
        while time.time() < deadline:
            row = store.get_export(self.conn, "E-1")
            if row["stage"] == "PUBLISHED":
                break
            time.sleep(0.05)
        time.sleep(0.5)  # give the loser a chance to misbehave
        stop.set()
        for t in threads:
            t.join()

        row = store.get_export(self.conn, "E-1")
        self.assertEqual("PUBLISHED", row["stage"])
        self.assertEqual(1, len(store.published_artifacts(self.conn, "E-1")))
        self.assertEqual(1, len(artifacts.list_published_files()))
        expected = hashlib.sha256(render_artifact_bytes(row)).hexdigest()
        self.assertEqual(expected, row["artifact_digest"])

    def test_tick_recovers_crashed_export_after_lease_expiry(self):
        """Simulate a crashed worker: staged artifact + expired lease -> tick converges."""
        os.environ["LEASE_TTL_SECONDS"] = "0.05"
        store.submit_export(self.conn, "E-1", [{"ts": "t0", "lat": 31.2, "depth_m": 10}])
        row = store.get_export(self.conn, "E-1")
        data = render_artifact_bytes(row)
        digest = hashlib.sha256(data).hexdigest()
        tmp = artifacts.tmp_path("E-1", "dead")
        artifacts.write_tmp(tmp, data)
        with store.immediate(self.conn):
            store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",))
            store.record_artifact(self.conn, "E-1", "staged", tmp, digest)
            store.cas_stage(self.conn, "E-1", "STAGED", ("PROCESSING",))
        # dead worker's lease, already expired
        store.acquire_lease(self.conn, worker.lease_resource("E-1"), "w-dead", 0.01)
        time.sleep(0.06)

        worker.tick(self.conn, "w-alive")

        row = store.get_export(self.conn, "E-1")
        self.assertEqual("PUBLISHED", row["stage"])
        self.assertEqual(digest, row["artifact_digest"])
        events = [e["event"] for e in store.export_events(self.conn, "E-1")]
        self.assertIn("published", events)


RECORDS = [{"ts": "t0", "lat": 31.2, "depth_m": 10}]


class LeaseHandoverTest(WorkerTestBase):
    """Deterministic lease-handover interleavings.

    The old worker is frozen at a named processing seam while its lease
    expires; a new worker takes over, recovers/reprocesses and publishes; then
    the old worker is released and must finish as ``lease_lost`` without
    leaving any staged record that points at a live temp file.
    """

    EXPORT = "E-HANDOVER"

    def setUp(self):
        super().setUp()
        os.environ["LEASE_TTL_SECONDS"] = "0.05"
        store.submit_export(self.conn, self.EXPORT, RECORDS)

    def _run_old_worker(self, gate_name):
        reached = threading.Event()
        release = threading.Event()

        def gate(me, export_id):
            if me == "w-old" and export_id == self.EXPORT:
                reached.set()
                release.wait(timeout=10)

        worker.install_hook(gate_name, gate)
        result = {}

        def old_worker():
            conn = store.connect()
            try:
                fencing = store.acquire_lease(
                    conn, worker.lease_resource(self.EXPORT), "w-old", config.lease_ttl()
                )
                result["fencing"] = fencing
                result["outcome"] = worker.process_export(conn, self.EXPORT, "w-old", fencing)
            finally:
                conn.close()

        thread = threading.Thread(target=old_worker)
        thread.start()
        self.assertTrue(reached.wait(5), "old worker never reached %s" % gate_name)
        return thread, result, release

    def _takeover_publishes(self):
        # Lease (50ms) is expired by the time the new worker ticks. Recovery
        # runs first, then a fresh RECEIVED export is processed end to end.
        time.sleep(0.08)
        self.assertTrue(worker.tick(self.conn, "w-new"))
        row = store.get_export(self.conn, self.EXPORT)
        self.assertEqual("PUBLISHED", row["stage"])

    def _assert_no_stale_temp_reference(self):
        row = store.get_export(self.conn, self.EXPORT)
        # published artifact is real and digest-verified
        data = artifacts.load_verified(row)
        self.assertEqual(hashlib.sha256(data).hexdigest(), row["artifact_digest"])
        self.assertEqual(1, len(store.published_artifacts(self.conn, self.EXPORT)))
        self.assertEqual(
            [artifacts.published_path(self.EXPORT)], artifacts.list_published_files()
        )
        # no temp files and no staged rows at all ...
        self.assertEqual([], artifacts.tmp_files_for(self.EXPORT))
        self.assertEqual([], store.staged_artifacts(self.conn, self.EXPORT))
        # ... and specifically no staged record referencing a real temp file
        for staged in self.conn.execute(
            "SELECT path FROM artifacts WHERE export_id = ? AND kind IN ('staged','aborted')",
            (self.EXPORT,),
        ).fetchall():
            self.assertFalse(os.path.exists(staged["path"]))

    def test_pause_before_first_temp_write_then_takeover_and_resume(self):
        """The reported interleaving: frozen before the first temp write."""
        thread, result, release = self._run_old_worker("before_tmp_write_gate")
        self._takeover_publishes()
        release.set()
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())

        self.assertEqual("lease_lost", result["outcome"])
        events = [e["event"] for e in store.export_events(self.conn, self.EXPORT)]
        self.assertIn("lease_lost", events)
        self._assert_no_stale_temp_reference()

    def test_pause_after_temp_write_then_takeover_and_resume(self):
        """Frozen after the temp write but before the staged record lands."""
        thread, result, release = self._run_old_worker("after_tmp_write")
        self._takeover_publishes()  # recovery removes the orphaned tmp + requeues
        release.set()
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())

        self.assertEqual("lease_lost", result["outcome"])
        events = [e["event"] for e in store.export_events(self.conn, self.EXPORT)]
        self.assertIn("lease_lost", events)
        self._assert_no_stale_temp_reference()

    def test_pause_after_staged_record_then_converge_and_resume(self):
        """Frozen after staging: takeover converges to that artifact and settles."""
        thread, result, release = self._run_old_worker("after_staged")
        self._takeover_publishes()  # recovery converges to the complete staged file
        release.set()
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())

        self.assertEqual("lease_lost", result["outcome"])
        self._assert_no_stale_temp_reference()


if __name__ == "__main__":
    unittest.main()
