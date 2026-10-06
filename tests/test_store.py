import os
import sqlite3
import tempfile
import time
import unittest

from app import store


class StoreTestBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        os.environ["DATA_DIR"] = self._tmp.name
        self.conn = store.connect()
        store.init_db(self.conn)

    def tearDown(self):
        self.conn.close()
        self._tmp.cleanup()
        os.environ.pop("DATA_DIR", None)

    def submit(self, export_id="E-1", records=None):
        records = records if records is not None else [{"ts": "t0", "lat": 31.2, "depth_m": 10}]
        return store.submit_export(self.conn, export_id, records)


class DecisionTest(StoreTestBase):
    def test_submit_freezes_records_and_rules_snapshot_atomically(self):
        status, receipt = self.submit()
        self.assertEqual(201, status)
        row = store.get_export(self.conn, "E-1")
        rules = store.get_rules(self.conn)
        self.assertEqual(row["rules_digest"], rules["digest"])
        self.assertEqual(row["rules_snapshot"], rules["body"])
        self.assertEqual(row["stage"], "RECEIVED")
        self.assertEqual(receipt["receipt_id"], row["receipt_id"])
        # journal entry persisted in the same decision
        events = store.export_events(self.conn, "E-1")
        self.assertEqual("received", events[-1]["event"])

    def test_business_equivalent_replay_returns_first_receipt(self):
        _, first = self.submit(records=[{"a": 1, "b": {"x": 1, "y": 2}}])
        # same business content, different key order / integral float
        status, second = self.submit(records=[{"b": {"y": 2, "x": 1.0}, "a": 1}])
        self.assertEqual(200, status)
        self.assertTrue(second["replay"])
        self.assertEqual(first["receipt_id"], second["receipt_id"])
        self.assertEqual(first["received_at"], second["received_at"])

    def test_different_records_conflict_and_preserve_evidence(self):
        _, first = self.submit()
        status, payload = self.submit(records=[{"ts": "t9", "lat": 1, "depth_m": 2}])
        self.assertEqual(409, status)
        self.assertEqual("conflict", payload["error"])
        self.assertEqual(first["input_digest"], payload["existing"]["input_digest"])
        self.assertNotEqual(payload["submitted"]["input_digest"], payload["existing"]["input_digest"])
        # original row untouched
        row = store.get_export(self.conn, "E-1")
        self.assertEqual(first["receipt_id"], row["receipt_id"])
        self.assertEqual("RECEIVED", row["stage"])

    def test_rules_change_turns_replay_into_conflict(self):
        self.submit()
        store.set_rules(self.conn, {"rules": [{"field": "depth_m", "action": "redact"}]})
        status, payload = self.submit()  # same records, but rules snapshot now differs
        self.assertEqual(409, status)
        self.assertNotEqual(payload["submitted"]["rules_digest"], payload["existing"]["rules_digest"])

    def test_new_export_after_rules_change_freezes_new_snapshot(self):
        self.submit("E-1")
        store.set_rules(self.conn, {"rules": [{"field": "depth_m", "action": "redact"}]})
        status, receipt = self.submit("E-2")
        self.assertEqual(201, status)
        self.assertEqual(receipt["rules_version"], 2)
        row = store.get_export(self.conn, "E-1")
        self.assertEqual(row["rules_version"], 1)  # E-1 stays frozen on v1


class StageTest(StoreTestBase):
    def test_forward_transitions(self):
        self.submit()
        self.assertTrue(store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",)))
        self.assertTrue(store.cas_stage(self.conn, "E-1", "STAGED", ("PROCESSING",)))
        self.assertTrue(store.mark_published(self.conn, "E-1", "d" * 64, "/p", "test", "unit"))
        row = store.get_export(self.conn, "E-1")
        self.assertEqual("PUBLISHED", row["stage"])
        self.assertEqual("d" * 64, row["artifact_digest"])

    def test_published_is_terminal(self):
        self.submit()
        store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",))
        store.mark_published(self.conn, "E-1", "d" * 64, "/p", "test", "unit")
        # no transition out of PUBLISHED is possible
        self.assertFalse(store.requeue(self.conn, "E-1", "test", "nope"))
        self.assertFalse(store.mark_published(self.conn, "E-1", "e" * 64, "/p2", "test", "unit"))
        row = store.get_export(self.conn, "E-1")
        self.assertEqual("PUBLISHED", row["stage"])
        self.assertEqual("d" * 64, row["artifact_digest"])

    def test_cas_refuses_wrong_source(self):
        self.submit()
        self.assertFalse(store.cas_stage(self.conn, "E-1", "STAGED", ("PROCESSING",)))
        self.assertEqual("RECEIVED", store.get_export(self.conn, "E-1")["stage"])

    def test_requeue_only_from_unfinished(self):
        self.submit()
        self.assertFalse(store.requeue(self.conn, "E-1", "t", "x"))  # RECEIVED not requeueable
        store.cas_stage(self.conn, "E-1", "PROCESSING", ("RECEIVED",))
        self.assertTrue(store.requeue(self.conn, "E-1", "t", "x"))
        row = store.get_export(self.conn, "E-1")
        self.assertEqual("RECEIVED", row["stage"])
        self.assertEqual(1, row["attempts"])

    def test_only_one_published_artifact_row(self):
        self.submit()
        self.assertTrue(store.record_artifact(self.conn, "E-1", "published", "/p", "d" * 64))
        self.assertFalse(store.record_artifact(self.conn, "E-1", "published", "/p2", "e" * 64))
        self.assertEqual(1, len(store.published_artifacts(self.conn, "E-1")))


class LeaseTest(StoreTestBase):
    def test_single_holder(self):
        self.assertIsNotNone(store.acquire_lease(self.conn, "r", "w1", 10))
        self.assertIsNone(store.acquire_lease(self.conn, "r", "w2", 10))

    def test_expired_lease_can_be_stolen_with_higher_fencing(self):
        f1 = store.acquire_lease(self.conn, "r", "w1", 0.05)
        time.sleep(0.08)
        f2 = store.acquire_lease(self.conn, "r", "w2", 10)
        self.assertIsNotNone(f2)
        self.assertGreater(f2, f1)
        # old holder no longer passes the fencing check
        self.assertFalse(store.check_lease(self.conn, "r", "w1", f1))
        self.assertTrue(store.check_lease(self.conn, "r", "w2", f2))

    def test_release_only_by_owner(self):
        fencing = store.acquire_lease(self.conn, "r", "w1", 10)
        store.release_lease(self.conn, "r", "w2", fencing)  # wrong owner: no-op
        self.assertIsNone(store.acquire_lease(self.conn, "r", "w2", 10))
        store.release_lease(self.conn, "r", "w1", fencing)
        self.assertIsNotNone(store.acquire_lease(self.conn, "r", "w2", 10))


class FaultTest(StoreTestBase):
    def test_fault_is_one_shot_and_mode_specific(self):
        store.set_fault(self.conn, "E-1", "crash_after_staged")
        self.assertFalse(store.pop_fault(self.conn, "E-1", "crash_partial_write"))
        self.assertTrue(store.pop_fault(self.conn, "E-1", "crash_after_staged"))
        self.assertFalse(store.pop_fault(self.conn, "E-1", "crash_after_staged"))


if __name__ == "__main__":
    unittest.main()
