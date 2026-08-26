"""The in-memory keyspace backing the server."""

from __future__ import annotations


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
    """

    def __init__(self) -> None:
        self._data: dict[str, str] = {}

    def get(self, key: str) -> str | None:
        """Return the value for `key`, or None if it isn't set."""
        return self._data.get(key)

    def set(self, key: str, value: str) -> None:
        """Set `key` to `value`, overwriting any existing value."""
        self._data[key] = value

    def delete(self, key: str) -> bool:
        """Remove `key`. Returns whether it existed."""
        if key in self._data:
            del self._data[key]
            return True
        return False
