import unittest

from app.masking import apply_rules, validate_rules


def rules(*entries):
    return {"rules": [dict(entry) for entry in entries]}


class ValidateTest(unittest.TestCase):
    def test_valid_document(self):
        self.assertEqual([], validate_rules(rules({"field": "a.b", "action": "redact"})))

    def test_rejects_non_object(self):
        self.assertTrue(validate_rules([1, 2, 3]))

    def test_rejects_empty_rules(self):
        self.assertTrue(validate_rules({"rules": []}))

    def test_rejects_bad_action(self):
        self.assertTrue(validate_rules(rules({"field": "a", "action": "obliterate"})))

    def test_rejects_bad_field(self):
        self.assertTrue(validate_rules(rules({"field": "a..b", "action": "drop"})))
        self.assertTrue(validate_rules(rules({"field": "1a", "action": "drop"})))

    def test_rejects_duplicate_field(self):
        doc = rules({"field": "a", "action": "drop"}, {"field": "a", "action": "redact"})
        self.assertTrue(validate_rules(doc))

    def test_rejects_bad_params(self):
        self.assertTrue(validate_rules(rules({"field": "a", "action": "round", "precision": 99})))
        self.assertTrue(validate_rules(rules({"field": "a", "action": "hash", "length": 2})))
        self.assertTrue(validate_rules(rules({"field": "a", "action": "redact", "replacement": 5})))


class ApplyTest(unittest.TestCase):
    def test_redact(self):
        doc = rules({"field": "depth_m", "action": "redact"})
        out = apply_rules([{"depth_m": 42.5, "lat": 1.0}], doc)
        self.assertEqual([{"depth_m": "***", "lat": 1.0}], out)

    def test_round(self):
        doc = rules({"field": "lat", "action": "round", "precision": 1})
        out = apply_rules([{"lat": 31.23456}], doc)
        self.assertAlmostEqual(31.2, out[0]["lat"])

    def test_hash_is_deterministic_and_truncated(self):
        doc = rules({"field": "vessel_id", "action": "hash", "length": 8})
        out1 = apply_rules([{"vessel_id": "HAICE-01"}], doc)
        out2 = apply_rules([{"vessel_id": "HAICE-01"}], doc)
        self.assertEqual(out1, out2)
        self.assertEqual(8, len(out1[0]["vessel_id"]))
        self.assertNotEqual("HAICE-01", out1[0]["vessel_id"])

    def test_drop(self):
        doc = rules({"field": "note", "action": "drop"})
        self.assertEqual([{}], apply_rules([{"note": "x"}], doc))

    def test_nested_path(self):
        doc = rules({"field": "pos.lat", "action": "redact"})
        out = apply_rules([{"pos": {"lat": 1.0, "lon": 2.0}}], doc)
        self.assertEqual({"pos": {"lat": "***", "lon": 2.0}}, out[0])

    def test_missing_field_is_noop(self):
        doc = rules({"field": "nope", "action": "drop"})
        self.assertEqual([{"a": 1}], apply_rules([{"a": 1}], doc))

    def test_input_not_mutated(self):
        doc = rules({"field": "a", "action": "drop"})
        record = {"a": 1}
        apply_rules([record], doc)
        self.assertEqual({"a": 1}, record)


if __name__ == "__main__":
    unittest.main()
