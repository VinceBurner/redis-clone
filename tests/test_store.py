import unittest
from unittest.mock import patch

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


class ExpiryTests(unittest.TestCase):
    """TTL behaviour, driven by a fake clock rather than real sleeping.

    Patches the `time` module *as bound inside store.py* rather than
    `time.monotonic` globally. Patching the real function would also affect
    the background server thread and selectors internals used by
    test_server.py, so the narrower target keeps the fake clock contained.
    """

    def setUp(self):
        self.store = DataStore()
        patcher = patch("redis_clone.store.time")
        self.mock_time = patcher.start()
        self.addCleanup(patcher.stop)
        self.now = 1000.0
        self.mock_time.monotonic.side_effect = lambda: self.now

    def advance(self, seconds):
        self.now += seconds

    # -- set with a TTL ----------------------------------------------------

    def test_get_before_expiry_returns_value(self):
        self.store.set("k", "v", ttl_seconds=10)
        self.advance(9)
        self.assertEqual(self.store.get("k"), "v")

    def test_get_after_expiry_returns_none(self):
        self.store.set("k", "v", ttl_seconds=10)
        self.advance(11)
        self.assertIsNone(self.store.get("k"))

    def test_key_expires_exactly_at_its_deadline(self):
        self.store.set("k", "v", ttl_seconds=10)
        self.advance(10)
        self.assertIsNone(self.store.get("k"))

    def test_set_without_ttl_has_no_expiry(self):
        self.store.set("k", "v")
        self.advance(10_000)
        self.assertEqual(self.store.get("k"), "v")

    def test_set_clears_a_previous_expiry(self):
        self.store.set("k", "v", ttl_seconds=10)
        self.store.set("k", "v2")  # no TTL given -> key becomes persistent
        self.advance(50)
        self.assertEqual(self.store.get("k"), "v2")
        self.assertEqual(self.store.ttl("k"), -1)

    def test_set_replaces_a_previous_expiry_with_a_new_one(self):
        self.store.set("k", "v", ttl_seconds=10)
        self.store.set("k", "v2", ttl_seconds=100)
        self.advance(50)
        self.assertEqual(self.store.get("k"), "v2")

    # -- expire() ----------------------------------------------------------

    def test_expire_on_existing_key_returns_true(self):
        self.store.set("k", "v")
        self.assertTrue(self.store.expire("k", 30))
        self.assertEqual(self.store.ttl("k"), 30)

    def test_expire_on_missing_key_returns_false(self):
        self.assertFalse(self.store.expire("nope", 30))

    def test_expire_on_already_expired_key_returns_false(self):
        self.store.set("k", "v", ttl_seconds=10)
        self.advance(11)
        # Must not resurrect the dead key.
        self.assertFalse(self.store.expire("k", 30))
        self.assertIsNone(self.store.get("k"))

    def test_expire_overwrites_an_existing_ttl(self):
        self.store.set("k", "v", ttl_seconds=10)
        self.assertTrue(self.store.expire("k", 100))
        self.advance(50)
        self.assertEqual(self.store.get("k"), "v")

    def test_expired_key_actually_goes_away_after_expire_call(self):
        self.store.set("k", "v", ttl_seconds=10)
        self.advance(11)
        self.store.expire("k", 30)
        self.assertEqual(self.store.ttl("k"), -2)

    # -- ttl() -------------------------------------------------------------

    def test_ttl_missing_key_returns_minus_two(self):
        self.assertEqual(self.store.ttl("nope"), -2)

    def test_ttl_expired_key_returns_minus_two(self):
        self.store.set("k", "v", ttl_seconds=10)
        self.advance(11)
        self.assertEqual(self.store.ttl("k"), -2)

    def test_ttl_key_without_expiry_returns_minus_one(self):
        self.store.set("k", "v")
        self.assertEqual(self.store.ttl("k"), -1)

    def test_ttl_counts_down(self):
        self.store.set("k", "v", ttl_seconds=100)
        self.assertEqual(self.store.ttl("k"), 100)
        self.advance(40)
        self.assertEqual(self.store.ttl("k"), 60)
        self.advance(59)
        self.assertEqual(self.store.ttl("k"), 1)

    def test_ttl_rounds_down_to_nearest_second(self):
        self.store.set("k", "v", ttl_seconds=10.9)
        self.assertEqual(self.store.ttl("k"), 10)
        self.advance(0.95)
        self.assertEqual(self.store.ttl("k"), 9)

    def test_ttl_never_returns_negative_for_a_live_key(self):
        self.store.set("k", "v", ttl_seconds=10)
        self.advance(9.999)
        self.assertEqual(self.store.ttl("k"), 0)

    # -- interaction with delete() -----------------------------------------

    def test_delete_on_expired_key_returns_false(self):
        self.store.set("k", "v", ttl_seconds=10)
        self.advance(11)
        self.assertFalse(self.store.delete("k"))

    def test_delete_clears_the_expiry_too(self):
        self.store.set("k", "v", ttl_seconds=10)
        self.assertTrue(self.store.delete("k"))
        # Re-created without a TTL; the old deadline must not still apply.
        self.store.set("k", "fresh")
        self.advance(50)
        self.assertEqual(self.store.get("k"), "fresh")


# NOTE: there is deliberately no concurrency test here. DataStore carries no
# lock because server.py drives it from a single-threaded event loop, so a
# multi-threaded test would assert a guarantee the class does not claim to
# make. See the class docstring in store.py for the full reasoning.


if __name__ == "__main__":
    unittest.main()
