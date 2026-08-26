import unittest

from redis_clone.store import DataStore


class DataStoreTests(unittest.TestCase):
    def setUp(self):
        self.store = DataStore()

    def test_get_missing_key_returns_none(self):
        self.assertIsNone(self.store.get("nope"))

    def test_set_then_get_round_trips(self):
        self.store.set("foo", "bar")
        self.assertEqual(self.store.get("foo"), "bar")

    def test_set_overwrites_existing_value(self):
        self.store.set("foo", "bar")
        self.store.set("foo", "baz")
        self.assertEqual(self.store.get("foo"), "baz")

    def test_keys_are_independent(self):
        self.store.set("a", "1")
        self.store.set("b", "2")
        self.assertEqual(self.store.get("a"), "1")
        self.assertEqual(self.store.get("b"), "2")

    def test_delete_existing_key_returns_true(self):
        self.store.set("foo", "bar")
        self.assertTrue(self.store.delete("foo"))
        self.assertIsNone(self.store.get("foo"))

    def test_delete_missing_key_returns_false(self):
        self.assertFalse(self.store.delete("nope"))

    def test_delete_is_not_idempotent_in_return_value(self):
        self.store.set("foo", "bar")
        self.assertTrue(self.store.delete("foo"))
        self.assertFalse(self.store.delete("foo"))

    def test_empty_string_value_round_trips(self):
        # Distinct from a missing key: get() must return "" not None.
        self.store.set("foo", "")
        self.assertEqual(self.store.get("foo"), "")


# NOTE: there is deliberately no concurrency test here. DataStore carries no
# lock because server.py drives it from a single-threaded event loop, so a
# multi-threaded test would assert a guarantee the class does not claim to
# make. See the class docstring in store.py for the full reasoning.


if __name__ == "__main__":
    unittest.main()
