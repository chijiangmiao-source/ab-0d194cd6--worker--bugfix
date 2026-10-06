import json
import unittest

from app.canonical import canonical, digest_of


class CanonicalTest(unittest.TestCase):
    def test_key_order_irrelevant(self):
        self.assertEqual(canonical({"b": 1, "a": 2}), canonical({"a": 2, "b": 1}))

    def test_whitespace_irrelevant(self):
        a = json.loads('{"a": 1, "b": [1, 2, 3]}')
        b = json.loads('{ "b":[1,2,3],"a":1 }')
        self.assertEqual(canonical(a), canonical(b))

    def test_nested_objects_normalized(self):
        a = {"outer": {"z": 1, "y": {"b": 2, "a": 1}}}
        b = {"outer": {"y": {"a": 1, "b": 2}, "z": 1}}
        self.assertEqual(canonical(a), canonical(b))

    def test_array_order_matters(self):
        self.assertNotEqual(canonical([1, 2]), canonical([2, 1]))

    def test_integral_floats_equal_ints(self):
        self.assertEqual(canonical({"lat": 31.0}), canonical({"lat": 31}))
        self.assertEqual(canonical({"lat": 31.10}), canonical({"lat": 31.1}))

    def test_unicode_nfc(self):
        composed = {"name": "café"}
        decomposed = {"name": "café"}
        self.assertEqual(canonical(composed), canonical(decomposed))

    def test_non_finite_rejected(self):
        for bad in (float("nan"), float("inf"), float("-inf")):
            with self.assertRaises(ValueError):
                canonical({"x": bad})

    def test_digest_stable(self):
        self.assertEqual(digest_of(canonical({"a": 1})), digest_of(canonical({"a": 1})))
        self.assertEqual(64, len(digest_of("x")))


if __name__ == "__main__":
    unittest.main()
