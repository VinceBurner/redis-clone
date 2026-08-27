# redis-clone

A minimal Redis server clone in Python, speaking the real [RESP](https://redis.io/docs/latest/develop/reference/protocol-spec/)
(REdis Serialization Protocol) over raw TCP sockets. Real Redis clients talk to
it unmodified, because it implements the wire format rather than a lookalike of
it.

Standard library only — no dependencies. `requirements.txt` is intentionally
empty. Runs on Python 3.9+.

```
redis_clone/
├── resp.py          # RESP parsing and encoding
├── store.py         # the keyspace, with lazy TTL expiry
├── server.py        # selectors event loop + command dispatch
├── requirements.txt # empty; stdlib only
└── tests/
    ├── test_resp.py    # 17 tests
    ├── test_store.py   # 27 tests
    └── test_server.py  # 21 tests, over real sockets
```

## Architecture: a single-threaded event loop

The server is one thread running a `selectors` event loop over non-blocking
sockets — the same shape as real Redis, and chosen for the same reason.

The obvious framing of thread-per-connection vs. event loop is "threads are
simpler, but you need a lock." That framing is wrong about what the lock buys
you, and the real reason is written up in full in the `DataStore` docstring in
[store.py](store.py). The short version:

**Locking each store method would give memory safety but not operation
atomicity.** A `Lock` inside `get`/`set`/`delete` prevents a corrupted dict. It
does *not* prevent two threads from interleaving between a locked `get` and a
locked `set`:

```
thread A: get("n") -> "5"          thread B: get("n") -> "5"
thread A: set("n", "6")            thread B: set("n", "6")   # one increment lost
```

Every individual call held the lock. The *operation* still raced. Any compound
command — `INCR`, `APPEND`, `GETSET`, `SETNX` — is broken by per-method locking,
and fixing it means holding a lock across whole-command dispatch. At that point
execution is serialized anyway, and the threads are paying context-switch and
lock overhead to reach what one thread does for free.

The event loop gets that serialization *structurally*: a command always runs to
completion before the loop looks at another client, so compound commands are
atomic by construction and there is no lock to forget later.

The costs are real and worth naming:

- **A slow command stalls every client.** Real Redis lives with this; it's why
  `KEYS *` is discouraged in production.
- **More code.** Non-blocking sockets need partial *writes* handled as well as
  partial reads, since `send()` can accept only part of a reply if a client
  stops draining. Each connection carries an `inbox` and an `outbox`, and
  `EVENT_WRITE` is registered only while output is pending.

Because the store is single-threaded by design, **nothing else may touch it from
another thread.** That constraint is what rules out a background expiry sweeper
(see below).

### Connection handling

Each client gets a `Connection` holding its socket plus two buffers. Commands
are parsed out of the inbox with `parse_command_with_length`, which returns
`None` when the buffer holds an incomplete message — so a command split across
several TCP segments simply waits for the rest rather than erroring. Pipelined
commands are drained in a loop, each producing its own reply.

Malformed (as opposed to merely incomplete) input gets a RESP error and closes
*that* connection only; the loop keeps serving everyone else.

## Supported commands

`PING`, `GET`, `SET`, `DEL`, `EXPIRE`, `TTL`. Command names are
case-insensitive. Anything else returns `-ERR unknown command '<name>'`, and a
wrong argument count returns `-ERR wrong number of arguments for '<cmd>'
command` without dropping the connection.

Below, `→` is what the client sends and `←` what the server replies.

### PING

```
→ *1\r\n$4\r\nPING\r\n
← +PONG\r\n
```

### SET key value

Always clears any existing TTL on the key, matching Redis's plain `SET`.

```
→ *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
← +OK\r\n
```

### GET key

Bulk string on a hit, null bulk string (`$-1`) on a miss or an expired key.

```
→ *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n
← $3\r\nbar\r\n

→ *2\r\n$3\r\nGET\r\n$7\r\nmissing\r\n
← $-1\r\n
```

### DEL key

Integer reply: `1` if the key existed, `0` otherwise. An expired key counts as
absent.

```
→ *2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n
← :1\r\n
```

### EXPIRE key seconds

Attaches a TTL to an existing key. `1` if the key was there to receive it, `0`
if it was absent or already expired. Non-numeric seconds return
`-ERR value is not an integer or out of range`.

```
→ *3\r\n$6\r\nEXPIRE\r\n$3\r\nfoo\r\n$2\r\n30\r\n
← :1\r\n
```

### TTL key

Integer reply: remaining whole seconds, `-1` if the key exists with no expiry,
`-2` if it doesn't exist (or has expired).

```
→ *2\r\n$3\r\nTTL\r\n$3\r\nfoo\r\n
← :30\r\n
```

### Known deviations from real Redis

- **`EXPIRE` accepts fractional seconds** (`0.1`). Real Redis rejects
  non-integer values here. `nan` and `inf` are rejected, since a NaN deadline
  compares `False` against every check and would silently produce an immortal
  key.
- **`DEL` takes exactly one key.** Real Redis is variadic.
- **`SET` takes no options.** No `EX`/`PX`/`NX`/`XX`. `DataStore.set()` does
  accept a `ttl_seconds` argument, but nothing on the wire reaches it — set a
  TTL with `EXPIRE`.

## Expiry is lazy, and only lazy

A key with a TTL stores an absolute deadline from `time.monotonic()` — not
`time.time()`, so an NTP step, a DST change, or a manual clock adjustment cannot
make a key expire early or late.

**Expired keys are evicted only when something touches them.** Every public
`DataStore` method calls `_drop_if_expired(key)` first; there is no timer and no
sweeper thread.

That is a deliberate consequence of the architecture above, not an oversight. A
background reaper would be a second thread mutating the store, which is exactly
what the no-lock design forbids. Adding one would reintroduce every
synchronization problem the event loop was chosen to avoid.

### The limitation this creates

**A key that expires and is never read again holds its memory indefinitely.**
Nothing will ever visit it to notice it is dead. A workload that writes many
short-TTL keys and never reads them back grows without bound.

**The fix, when it's needed:** active expiry *sampling on the event loop
thread* — periodically check a small random sample of keys with TTLs between
`select()` calls and drop the dead ones. That's what real Redis does to bound
the same problem, and because it runs on the loop's own thread it stays
compatible with the single-threaded design instead of breaking it. Not
implemented here.

## Running it

From the **parent** directory (the one *containing* `redis_clone/`):

```sh
python3 -m redis_clone.server
```

```
redis-clone listening on localhost:6380
```

**This is not interchangeable with `python3 server.py`.** The modules use
package-relative imports (`from .store import DataStore`), which only resolve
when Python loads the file as part of the `redis_clone` package. Running the
script directly from inside the directory fails with
`ImportError: attempted relative import with no known parent package`. The `-m`
form from the parent directory is the supported invocation.

It listens on **port 6380**, not 6379, so it won't collide with a real Redis.
Stop it with Ctrl-C.

### Testing it manually with nc

`redis-cli` is not installed in this environment, so these examples send raw
RESP bytes. Note that macOS `nc` uses `-w` for its timeout, not the `-q` you'll
see in most Linux-oriented examples.

A single `PING`, with `xxd` to make the CRLFs visible:

```sh
printf '*1\r\n$4\r\nPING\r\n' | nc -w1 localhost 6380 | xxd
```
```
00000000: 2b50 4f4e 470d 0a                        +PONG..
```

Two pipelined commands on one connection — `SET foo bar` then `GET foo` — which
also demonstrates that each gets its own reply:

```sh
printf '*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n' \
  | nc -w1 localhost 6380 | xxd
```
```
00000000: 2b4f 4b0d 0a24 330d 0a62 6172 0d0a       +OK..$3..bar..
```

For an interactive session where you type commands by hand, plain
`nc localhost 6380` works, but you must send real CRLF line endings — most
terminals send bare `\n`, which the parser will treat as an incomplete message
and wait on. Driving `nc` from a FIFO (`mkfifo`, then `printf ... >&3`) is the
reliable way to hold a connection open and send byte-exact commands at chosen
moments.

## Running the tests

From the same parent directory:

```sh
python3 -m unittest discover -v
```

**65 tests**, all passing, in about half a second. Discovery only descends into
importable packages, so it finds `redis_clone/tests/` without scanning
unrelated directories.

| File | Count | Covers |
| --- | --- | --- |
| `test_resp.py` | 17 | Parsing well-formed commands at several argument counts; incomplete input returning `None` at *every* truncation point rather than raising; malformed input raising; byte-exact output from all four encoders including the `$-1` null bulk string and negative integers. |
| `test_store.py` | 27 | `get`/`set`/`delete` semantics including empty-string values (distinct from missing); and 19 expiry tests — TTL countdown, rounding down, `-1`/`-2` sentinels, expiry at the exact deadline, `SET` clearing a prior TTL, and `EXPIRE` refusing to resurrect an already-expired key. |
| `test_server.py` | 21 | End-to-end over **real TCP sockets**, not mocks: every command, arity and protocol errors, pipelining, TTL expiry observed across two clients, and concurrency. |

Two details worth knowing if you edit these:

- **The expiry tests use a fake clock, not `sleep`.** They patch the `time`
  module *as bound inside `store.py`* rather than patching `time.monotonic`
  globally — a global patch would also hit the background server thread and
  `selectors`' internals during `test_server.py`. All 19 run in ~4ms.
- **There is deliberately no threaded concurrency test for `DataStore`.** It
  carries no lock by design, so such a test would assert a guarantee the class
  does not make.

The load-bearing concurrency test is
`test_client_stalled_mid_command_does_not_block_another`: one client sends
*half* a command and goes silent, and the test proves a second client still
completes full round trips — then that the first client's command finishes
correctly once its remaining bytes arrive. A blocking single-connection server
fails this test.

Integration tests bind port 0 (OS-assigned), so they never collide with a
server you have running on 6380.

## Out of scope

This is a learning implementation of the protocol and event loop, not a Redis
replacement. Not implemented, and not planned here:

- **Persistence** — no RDB snapshots, no AOF. Everything is in memory and dies
  with the process.
- **Replication and clustering** — no primary/replica, no sharding, no Sentinel.
- **Data types beyond strings** — no lists, hashes, sets, sorted streams, or
  bitmaps. The store is `dict[str, str]`.
- **Active expiry sampling** — expiry is lazy-only; see the limitation above.
- **Transactions and scripting** — no `MULTI`/`EXEC`/`WATCH`, no Lua.
- **Pub/sub, keyspace notifications, blocking commands.**
- **Auth and multiple databases** — no `AUTH`, no `SELECT`; one keyspace,
  no access control. It binds `localhost` and should not be exposed to a
  network.
