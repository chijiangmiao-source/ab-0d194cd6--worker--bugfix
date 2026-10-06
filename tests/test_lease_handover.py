"""Lease-handover regression (STALE_LEASE_TEMP_LEAK).

A worker that resumes after its lease was taken over must not leave temp
artifacts behind: it must not write or register anything itself, and whatever
it staged before losing the lease must be cleaned up by the takeover/publish.

The interleavings are deterministic: worker pause hooks let the test run the
takeover (lease expiry -> recovery -> reprocess -> publish) while the old
worker is suspended, then resume the old worker.
"""
import hashlib
import os
import tempfile
import time
import unittest

from app import artifacts, config, store, worker
from app.render import render_artifact_bytes

RECORDS = [{"ts": "t0", "lat": 31.2, "depth_m": 10}]
OLD_LEASE_TTL = "0.05"  # 50ms, as in the reported reproduction


class LeaseHandoverTestBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        os.environ["DATA_DIR"] = self._tmp.name
        os.environ["LEASE_TTL_SECONDS"] = OLD_LEASE_TTL
        config.ensure_dirs()
        self.conn = store.connect()
        store.init_db(self.conn)
        store.submit_export(self.conn, "E-1", RECORDS)

    def tearDown(self):
        self.conn.close()
        self._tmp.cleanup()
        for var in ("DATA_DIR", "LEASE_TTL_SECONDS"):
            os.environ.pop(var, None)

    def acquire_old_lease(self):
        fencing = store.acquire_lease(self.conn, worker.lease_resource("E-1"), "w-old", 0.05)
        self.assertIsNotNone(fencing)
        return fencing

    def new_worker_tick(self):
        conn2 = store.connect()
        try:
            worker.tick(conn2, "w-new")
        finally:
            conn2.close()

    def assert_published_artifact(self):
        row = store.get_export(self.conn, "E-1")
        self.assertEqual("PUBLISHED", row["stage"])
        expected = hashlib.sha256(render_artifact_bytes(row)).hexdigest()
        self.assertEqual(expected, row["artifact_digest"])
        data = artifacts.load_verified(row)  # digest verified
        self.assertEqual(expected, hashlib.sha256(data).hexdigest())
        self.assertEqual(1, len(store.published_artifacts(self.conn, "E-1")))
        return row

    def assert_no_temp_leak(self):
        self.assertEqual([], artifacts.list_tmp_files())
        for rec in store.staged_artifacts(self.conn, "E-1"):
            self.assertFalse(
                os.path.exists(rec["path"]),
                "staged record still references a temp file: %s" % rec["path"],
            )


class PauseBeforeTmpWriteTest(LeaseHandoverTestBase):
    def test_stale_worker_resuming_after_handover_registers_nothing(self):
        """Old worker pauses before the first temp write; new worker takes
        over after the 50ms lease expiry and publishes; old worker resumes."""
        fencing_old = self.acquire_old_lease()
        takeover = []

        def pause_then_takeover(export_id, me):
            if me != "w-old" or takeover:
                return  # fire only for the old worker's first pause
            # While the old worker is paused its lease expires; the new
            # worker takes the export over: recover -> reprocess -> publish.
            time.sleep(0.1)  # the 50ms lease is now expired
            os.environ["LEASE_TTL_SECONDS"] = "5"  # takeover need not race the TTL
            self.new_worker_tick()
            takeover.append(True)

        worker.before_tmp_write_hook = pause_then_takeover
        try:
            result = worker.process_export(self.conn, "E-1", "w-old", fencing_old)
        finally:
            worker.before_tmp_write_hook = None

        self.assertTrue(takeover, "lease handover must happen during the pause")
        # the stale worker detects the lost lease and stops without writing
        self.assertEqual("lease_lost", result)
        self.assert_published_artifact()
        events = [(e["actor"], e["event"]) for e in store.export_events(self.conn, "E-1")]
        self.assertIn(("w-old", "lease_lost"), events)
        self.assertIn(("w-new", "published"), events)
        self.assert_no_temp_leak()


class LeaseStolenDuringTmpWriteTest(LeaseHandoverTestBase):
    def test_staged_registration_is_fenced(self):
        """Lease stolen between the temp write and the staged registration:
        the registration must not commit and the temp file must be dropped."""
        fencing_old = self.acquire_old_lease()
        stolen = []

        def steal_lease(export_id, me):
            if me != "w-old" or stolen:
                return
            time.sleep(0.1)  # let the old lease expire
            conn2 = store.connect()
            try:
                fencing = store.acquire_lease(conn2, worker.lease_resource("E-1"), "w-new", 5)
                self.assertIsNotNone(fencing)
            finally:
                conn2.close()
            stolen.append(True)

        worker.after_tmp_write_hook = steal_lease
        try:
            result = worker.process_export(self.conn, "E-1", "w-old", fencing_old)
        finally:
            worker.after_tmp_write_hook = None

        self.assertTrue(stolen, "lease must be stolen during the temp write")
        self.assertEqual("lease_lost", result)
        # the stale worker registered nothing and removed its own temp file
        self.assert_no_temp_leak()
        # the new owner takes over and publishes cleanly
        os.environ["LEASE_TTL_SECONDS"] = "5"
        self.new_worker_tick()
        self.assert_published_artifact()
        self.assert_no_temp_leak()


class TakeoverCleanupTest(LeaseHandoverTestBase):
    def test_takeover_converges_and_clears_staged_records(self):
        """The old worker staged a complete artifact before vanishing: the
        takeover converges on the same bytes and clears staged records/temp
        files, so nothing referenceable is left after publish."""
        row = store.get_export(self.conn, "E-1")
        data = render_artifact_bytes(row)
        digest = hashlib.sha256(data).hexdigest()
        tmp = artifacts.tmp_path("E-1", "stale")
        artifacts.write_tmp(tmp, data)
        self.acquire_old_lease()
        with store.immediate(self.conn):
            store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",))
            store.record_artifact(self.conn, "E-1", "staged", tmp, digest)
            store.cas_stage(self.conn, "E-1", "STAGED", ("PROCESSING",))
        time.sleep(0.1)  # old lease expires; old worker never comes back

        self.new_worker_tick()

        row = self.assert_published_artifact()
        self.assertEqual(digest, row["artifact_digest"])  # converged on the same bytes
        self.assertEqual([], store.staged_artifacts(self.conn, "E-1"))
        self.assert_no_temp_leak()


if __name__ == "__main__":
    unittest.main()
