import hashlib
import os
import tempfile
import unittest

from app import artifacts, config, recovery, store
from app.render import render_artifact_bytes


class RecoveryTestBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        os.environ["DATA_DIR"] = self._tmp.name
        config.ensure_dirs()
        self.conn = store.connect()
        store.init_db(self.conn)
        store.submit_export(self.conn, "E-1", [{"ts": "t0", "lat": 31.2, "depth_m": 10}])

    def tearDown(self):
        self.conn.close()
        self._tmp.cleanup()
        os.environ.pop("DATA_DIR", None)

    def export(self):
        return store.get_export(self.conn, "E-1")

    def expected_digest(self):
        return hashlib.sha256(render_artifact_bytes(self.export())).hexdigest()


class ConvergeTest(RecoveryTestBase):
    def test_complete_staged_artifact_is_published_as_is(self):
        """Crash after staging: recovery converges to the same complete artifact."""
        data = render_artifact_bytes(self.export())
        digest = hashlib.sha256(data).hexdigest()
        tmp = artifacts.tmp_path("E-1", "deadbeef")
        artifacts.write_tmp(tmp, data)
        with store.immediate(self.conn):
            store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",))
            store.record_artifact(self.conn, "E-1", "staged", tmp, digest)
            store.cas_stage(self.conn, "E-1", "STAGED", ("PROCESSING",))

        result = recovery.recover_export(self.conn, "E-1", "test")

        self.assertEqual("converged", result)
        row = self.export()
        self.assertEqual("PUBLISHED", row["stage"])
        self.assertEqual(digest, row["artifact_digest"])
        # the very same bytes were published, not a regenerated copy
        with open(artifacts.published_path("E-1"), "rb") as fh:
            self.assertEqual(data, fh.read())
        self.assertEqual([], artifacts.tmp_files_for("E-1"))

    def test_published_file_present_but_db_not_updated(self):
        """Crash between atomic link and DB update: converge bookkeeping."""
        data = render_artifact_bytes(self.export())
        digest = hashlib.sha256(data).hexdigest()
        tmp = artifacts.tmp_path("E-1", "cafe")
        artifacts.write_tmp(tmp, data)
        artifacts.publish(tmp, artifacts.published_path("E-1"), digest)
        with store.immediate(self.conn):
            store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",))
            store.cas_stage(self.conn, "E-1", "STAGED", ("PROCESSING",))

        result = recovery.recover_export(self.conn, "E-1", "test")

        self.assertEqual("converged", result)
        row = self.export()
        self.assertEqual("PUBLISHED", row["stage"])
        self.assertEqual(digest, row["artifact_digest"])
        self.assertEqual(1, len(store.published_artifacts(self.conn, "E-1")))


class CleanupTest(RecoveryTestBase):
    def test_partial_write_is_cleaned_and_requeued(self):
        """Crash mid-write: partial temp file removed, export back to RECEIVED."""
        data = render_artifact_bytes(self.export())
        tmp = artifacts.tmp_path("E-1", "half")
        artifacts.write_tmp(tmp, data[: len(data) // 2])
        with store.immediate(self.conn):
            store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",))

        result = recovery.recover_export(self.conn, "E-1", "test")

        self.assertEqual("requeued", result)
        row = self.export()
        self.assertEqual("RECEIVED", row["stage"])
        self.assertEqual(1, row["attempts"])
        self.assertEqual([], artifacts.tmp_files_for("E-1"))

    def test_corrupted_staged_artifact_is_aborted(self):
        data = render_artifact_bytes(self.export())
        digest = hashlib.sha256(data).hexdigest()
        tmp = artifacts.tmp_path("E-1", "badc0de")
        artifacts.write_tmp(tmp, data + b"corruption")
        with store.immediate(self.conn):
            store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",))
            store.record_artifact(self.conn, "E-1", "staged", tmp, digest)
            store.cas_stage(self.conn, "E-1", "STAGED", ("PROCESSING",))

        result = recovery.recover_export(self.conn, "E-1", "test")

        self.assertEqual("requeued", result)
        self.assertEqual([], artifacts.tmp_files_for("E-1"))
        self.assertEqual([], store.staged_artifacts(self.conn, "E-1"))
        self.assertEqual("RECEIVED", self.export()["stage"])

    def test_published_export_is_never_touched(self):
        with store.immediate(self.conn):
            store.mark_published(self.conn, "E-1", "d" * 64, "/nowhere", "test", "unit")
        self.assertEqual("none", recovery.recover_export(self.conn, "E-1", "test"))
        self.assertEqual("PUBLISHED", self.export()["stage"])
        self.assertEqual("d" * 64, self.export()["artifact_digest"])

    def test_orphan_sweep_removes_unreferenced_old_temp_files(self):
        orphan = artifacts.tmp_path("E-9", "orphan")
        artifacts.write_tmp(orphan, b"leftover")
        old = 1_600_000_000
        os.utime(orphan, (old, old))
        removed = recovery.sweep_orphans(self.conn, "test", older_than_seconds=1)
        self.assertIn(orphan, removed)
        self.assertFalse(os.path.exists(orphan))


class SettlePublishedTest(RecoveryTestBase):
    def test_leftover_staged_record_after_publication_is_settled(self):
        """Reproduces STALE_LEASE_TEMP_LEAK: a PUBLISHED export must never keep
        a 'staged' record that points at a temp file still on disk."""
        data = render_artifact_bytes(self.export())
        digest = hashlib.sha256(data).hexdigest()
        # Current owner published normally ...
        published_tmp = artifacts.tmp_path("E-1", "owner")
        artifacts.write_tmp(published_tmp, data)
        artifacts.publish(published_tmp, artifacts.published_path("E-1"), digest)
        with store.immediate(self.conn):
            store.record_artifact(self.conn, "E-1", "published",
                                  artifacts.published_path("E-1"), digest)
            store.mark_published(self.conn, "E-1", digest,
                                 artifacts.published_path("E-1"), "w-new", "linked")
        # ... while a superseded worker had left a staged row + live temp file.
        stale = artifacts.tmp_path("E-1", "stale")
        artifacts.write_tmp(stale, data)
        with store.immediate(self.conn):
            store.record_artifact(self.conn, "E-1", "staged", stale, digest)

        report = recovery.settle_published(self.conn, "E-1", "w-new")

        self.assertEqual(1, report["aborted"])
        self.assertIn(stale, report["removed"])
        self.assertFalse(os.path.exists(stale))
        self.assertEqual([], store.staged_artifacts(self.conn, "E-1"))
        self.assertEqual("PUBLISHED", self.export()["stage"])
        self.assertEqual([], artifacts.tmp_files_for("E-1"))

    def test_settle_is_a_noop_before_publication(self):
        data = render_artifact_bytes(self.export())
        tmp = artifacts.tmp_path("E-1", "early")
        artifacts.write_tmp(tmp, data)
        with store.immediate(self.conn):
            store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",))
            store.record_artifact(self.conn, "E-1", "staged", tmp,
                                  hashlib.sha256(data).hexdigest())
        report = recovery.settle_published(self.conn, "E-1", "w-new")
        self.assertEqual({"aborted": 0, "removed": []}, report)
        self.assertTrue(os.path.exists(tmp))  # in-flight staging untouched


class PublishPrimitiveTest(RecoveryTestBase):
    def test_publish_never_clobbers(self):
        a = artifacts.tmp_path("E-1", "a")
        b = artifacts.tmp_path("E-1", "b")
        artifacts.write_tmp(a, b"same-content")
        artifacts.write_tmp(b, b"same-content")
        dst = artifacts.published_path("E-1")
        digest = artifacts.sha256_bytes(b"same-content")
        self.assertEqual("linked", artifacts.publish(a, dst, digest))
        self.assertEqual("dedup", artifacts.publish(b, dst, digest))
        self.assertFalse(os.path.exists(a))
        self.assertFalse(os.path.exists(b))
        self.assertEqual(1, len(artifacts.list_published_files()))

    def test_publish_refuses_different_content(self):
        a = artifacts.tmp_path("E-1", "a")
        b = artifacts.tmp_path("E-1", "b")
        artifacts.write_tmp(a, b"content-a")
        artifacts.write_tmp(b, b"content-b")
        dst = artifacts.published_path("E-1")
        artifacts.publish(a, dst, artifacts.sha256_bytes(b"content-a"))
        with self.assertRaises(artifacts.PublishedMismatch):
            artifacts.publish(b, dst, artifacts.sha256_bytes(b"content-b"))

    def test_load_verified_rejects_tampered_bytes(self):
        data = render_artifact_bytes(self.export())
        digest = hashlib.sha256(data).hexdigest()
        with store.immediate(self.conn):
            store.mark_published(self.conn, "E-1", digest, artifacts.published_path("E-1"), "t", "unit")
        artifacts.write_tmp(artifacts.published_path("E-1"), data + b"tamper")
        with self.assertRaises(artifacts.DigestMismatch):
            artifacts.load_verified(self.export())


if __name__ == "__main__":
    unittest.main()
