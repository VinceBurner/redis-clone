"""The in-memory keyspace backing the server."""

from __future__ import annotations

import re
import time

# Redis's own wording, reused verbatim so clients see the familiar text.
NOT_AN_INTEGER = "value is not an integer or out of range"

# Explicit [0-9] rather than \d: \d also matches non-ASCII digits, and
# Python's int() additionally accepts underscores ("1_0" -> 10) and
# surrounding whitespace. Redis accepts none of those, so parse strictly.
_INTEGER = re.compile(r"^-?[0-9]+$")


class DataStore:
    """A dict wrapper holding every key the server knows about.

    Deliberately *unsynchronized* — there is no lock here, and that is a
    design choice rather than an oversight.

    server.py runs a single-threaded `selectors` event loop, so exactly one
    thread ever touches this dict. A lock would protect against contention
    that cannot occur, and would cost an acquire/release on every command.

    The deeper reason is atomicity, not memory safety. Locking each method
    below would make individual calls safe but would NOT make multi-step
    commands safe: a future INCR (get, add one, set) run from two threads
    could interleave between its get and its set and lose an increment,
    even though both calls held the lock. Guarding that requires holding a
    lock across whole-command dispatch, which serializes execution anyway.
    The event loop gets that same serialization structurally and for free:
    a command always runs to completion before the loop looks at another
    client, so compound commands are atomic by construction.

    Consequence: if this class is ever shared across threads, it needs a
    lock around whole commands (not around these methods), or it is unsafe.

    Expiry is *lazy* for exactly that reason: a key is dropped only when it
    is next accessed, never by a background sweeper. A timer thread reaping
    expired keys would be a second thread touching this dict, which is the
    one thing the no-lock design above rules out. The cost is that an
    expired key nobody touches keeps occupying memory until it is read;
    real Redis pairs lazy expiry with an active sampling cycle *inside* its
    event loop to bound that, which is where this would go later.

    Deadlines use time.monotonic(), not time.time(), so a wall-clock
    adjustment (NTP step, DST, manual change) cannot make a key expire
    early or late.
    """

    def __init__(self) -> None:
        self._data: dict[str, str] = {}
        # key -> absolute time.monotonic() deadline. Only keys with a TTL
        # appear here; absence means "no expiry".
        self._expiries: dict[str, float] = {}

    # -- expiry helpers ----------------------------------------------------

    def _drop_if_expired(self, key: str) -> None:
        """Evict `key` if its deadline has passed.

        Every public method calls this first, which is what makes expiry
        lazy: the check happens on access, not on a timer.
        """
        deadline = self._expiries.get(key)
        if deadline is not None and time.monotonic() >= deadline:
            self._data.pop(key, None)
            self._expiries.pop(key, None)

    # -- commands ----------------------------------------------------------

    def get(self, key: str) -> str | None:
        """Return the value for `key`, or None if unset or expired."""
        self._drop_if_expired(key)
        return self._data.get(key)

    def set(self, key: str, value: str, ttl_seconds: float | None = None) -> None:
        """Set `key` to `value`, overwriting any existing value.

        A write always clears any previous expiry: passing no ttl_seconds
        makes the key persistent again, matching how Redis's plain SET
        discards the old TTL.
        """
        self._data[key] = value
        if ttl_seconds is None:
            self._expiries.pop(key, None)
        else:
            self._expiries[key] = time.monotonic() + ttl_seconds

    def delete(self, key: str) -> bool:
        """Remove `key`. Returns whether it existed (expired counts as not)."""
        self._drop_if_expired(key)
        if key in self._data:
            del self._data[key]
            self._expiries.pop(key, None)
            return True
        return False

    def increment(self, key: str, delta: int) -> int:
        """Add `delta` to the integer at `key` and return the new value.

        A missing (or expired) key counts as 0, so INCR on an absent key
        yields 1 and DECR yields -1. Raises ValueError if the stored value
        is not an integer.

        On why this needs no extra locking: it doesn't, and that is not a
        new guarantee — it is the existing one applying to a two-step
        operation. This method still only ever runs on the single
        event-loop thread, so the read-then-write below cannot interleave
        with another client's read-then-write; the loop finishes this
        command before it looks at another socket. Note that the class
        docstring uses exactly this operation as the example of what
        per-method locking would *fail* to protect: under threads, a lock
        around get() and a lock around set() would still let two
        increments interleave and lose one. Single-threaded execution is
        what makes the sequence atomic, not any lock.

        Writes straight to _data rather than calling set(), because set()
        deliberately clears the expiry and an increment must preserve it
        (matching real Redis, where INCR leaves the TTL alone).
        """
        self._drop_if_expired(key)

        current = self._data.get(key)
        if current is None:
            value = 0
        else:
            if not _INTEGER.match(current):
                raise ValueError(NOT_AN_INTEGER)
            value = int(current)

        new_value = value + delta
        self._data[key] = str(new_value)  # _expiries left untouched
        return new_value

    def expire(self, key: str, ttl_seconds: float) -> bool:
        """Attach a new expiry to an existing key.

        Returns True if the key was there to be given one. An already-expired
        key is treated as absent, so this returns False rather than
        resurrecting it.
        """
        self._drop_if_expired(key)
        if key not in self._data:
            return False
        self._expiries[key] = time.monotonic() + ttl_seconds
        return True

    def ttl(self, key: str) -> int:
        """Seconds left on `key`: -2 if absent/expired, -1 if no expiry.

        Otherwise the remaining time rounded down, floored at 0.
        """
        self._drop_if_expired(key)
        if key not in self._data:
            return -2
        deadline = self._expiries.get(key)
        if deadline is None:
            return -1
        return max(0, int(deadline - time.monotonic()))
