"""Integration tests driving the server over real TCP sockets."""

from __future__ import annotations

import socket
import threading
import unittest

from redis_clone.server import Server

TIMEOUT = 5  # seconds; a hang should fail the suite, not stall it


def encode_command(*args: str) -> bytes:
    """Build a RESP Array of Bulk Strings, the way a real client would."""
    out = f"*{len(args)}\r\n".encode()
    for arg in args:
        encoded = arg.encode()
        out += f"${len(encoded)}\r\n".encode() + encoded + b"\r\n"
    return out


class Client:
    """A minimal blocking RESP client, so tests use real sockets not mocks."""

    def __init__(self, port: int) -> None:
        self.sock = socket.create_connection(("localhost", port), TIMEOUT)
        self.sock.settimeout(TIMEOUT)
        self.buffer = b""

    def send_raw(self, data: bytes) -> None:
        self.sock.sendall(data)

    def command(self, *args: str) -> bytes:
        """Send one command and return its raw reply bytes."""
        self.send_raw(encode_command(*args))
        return self.read_reply()

    def read_reply(self) -> bytes:
        while True:
            reply = self._take_reply()
            if reply is not None:
                return reply
            chunk = self.sock.recv(4096)
            if not chunk:
                raise AssertionError("server closed the connection")
            self.buffer += chunk

    def _take_reply(self) -> bytes | None:
        """Pull one complete reply off the buffer, or None if incomplete."""
        buf = self.buffer
        if not buf:
            return None

        end = buf.find(b"\r\n")
        if end == -1:
            return None

        kind = buf[:1]
        if kind in (b"+", b"-", b":"):
            total = end + 2
        elif kind == b"$":
            length = int(buf[1:end])
            total = end + 2 if length == -1 else end + 2 + length + 2
        else:
            raise AssertionError(f"unexpected reply type: {kind!r}")

        if len(buf) < total:
            return None
        self.buffer = buf[total:]
        return buf[:total]

    def close(self) -> None:
        self.sock.close()


class ServerIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Port 0 lets the OS pick a free port, so tests never collide with a
        # real server (or another test run) on 6380.
        cls.server = Server(host="localhost", port=0)
        cls.server.start()
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()
        cls.thread.join(timeout=TIMEOUT)

    def connect(self) -> Client:
        client = Client(self.server.port)
        self.addCleanup(client.close)
        return client

    # -- single client behaviour ------------------------------------------

    def test_ping(self):
        self.assertEqual(self.connect().command("PING"), b"+PONG\r\n")

    def test_set_then_get_round_trips(self):
        client = self.connect()
        self.assertEqual(client.command("SET", "rt", "hello"), b"+OK\r\n")
        self.assertEqual(client.command("GET", "rt"), b"$5\r\nhello\r\n")

    def test_get_missing_key_returns_null_bulk_string(self):
        self.assertEqual(self.connect().command("GET", "absent"), b"$-1\r\n")

    def test_del_existing_key_returns_one(self):
        client = self.connect()
        client.command("SET", "doomed", "x")
        self.assertEqual(client.command("DEL", "doomed"), b":1\r\n")
        self.assertEqual(client.command("GET", "doomed"), b"$-1\r\n")

    def test_del_missing_key_returns_zero(self):
        self.assertEqual(self.connect().command("DEL", "never"), b":0\r\n")

    def test_lowercase_commands_work(self):
        client = self.connect()
        self.assertEqual(client.command("set", "lower", "v"), b"+OK\r\n")
        self.assertEqual(client.command("get", "lower"), b"$1\r\nv\r\n")

    # -- error handling ----------------------------------------------------

    def test_wrong_arity_returns_error_without_crashing(self):
        client = self.connect()
        cases = [
            (("GET",), b"-ERR wrong number of arguments for 'get' command\r\n"),
            (("GET", "a", "b"), b"-ERR wrong number of arguments for 'get' command\r\n"),
            (("SET", "a"), b"-ERR wrong number of arguments for 'set' command\r\n"),
            (("DEL",), b"-ERR wrong number of arguments for 'del' command\r\n"),
        ]
        for args, expected in cases:
            with self.subTest(args=args):
                self.assertEqual(client.command(*args), expected)
        # The connection survives every one of those errors.
        self.assertEqual(client.command("PING"), b"+PONG\r\n")

    def test_unknown_command_returns_error(self):
        client = self.connect()
        self.assertEqual(
            client.command("NOPE"), b"-ERR unknown command 'NOPE'\r\n"
        )
        self.assertEqual(client.command("PING"), b"+PONG\r\n")

    def test_pipelined_commands_get_separate_replies(self):
        client = self.connect()
        client.send_raw(encode_command("SET", "pipe", "1") + encode_command("GET", "pipe"))
        self.assertEqual(client.read_reply(), b"+OK\r\n")
        self.assertEqual(client.read_reply(), b"$1\r\n1\r\n")

    # -- concurrency -------------------------------------------------------

    def test_two_clients_use_the_store_independently(self):
        a = self.connect()
        b = self.connect()

        self.assertEqual(a.command("SET", "key_a", "from_a"), b"+OK\r\n")
        self.assertEqual(b.command("SET", "key_b", "from_b"), b"+OK\r\n")

        # Each sees its own write...
        self.assertEqual(a.command("GET", "key_a"), b"$6\r\nfrom_a\r\n")
        self.assertEqual(b.command("GET", "key_b"), b"$6\r\nfrom_b\r\n")
        # ...and the other's, since the keyspace is shared.
        self.assertEqual(a.command("GET", "key_b"), b"$6\r\nfrom_b\r\n")
        self.assertEqual(b.command("GET", "key_a"), b"$6\r\nfrom_a\r\n")

    def test_client_stalled_mid_command_does_not_block_another(self):
        """The core win over the day-one blocking server.

        Client A sends only *half* a command and then goes quiet. The old
        single-connection loop would sit in recv() waiting for A's remaining
        bytes forever, starving everyone else. The event loop must keep
        serving B, and must still finish A's command once the rest arrives.
        """
        a = self.connect()
        b = self.connect()

        partial = encode_command("SET", "stalled", "value")
        split = len(partial) // 2
        a.send_raw(partial[:split])  # A is now mid-command and silent

        # B is completely unaffected.
        self.assertEqual(b.command("SET", "b_key", "b_val"), b"+OK\r\n")
        self.assertEqual(b.command("GET", "b_key"), b"$5\r\nb_val\r\n")
        self.assertEqual(b.command("PING"), b"+PONG\r\n")

        # A's buffered half is still intact; finishing it completes the command.
        a.send_raw(partial[split:])
        self.assertEqual(a.read_reply(), b"+OK\r\n")
        self.assertEqual(b.command("GET", "stalled"), b"$5\r\nvalue\r\n")

    def test_many_clients_interleaved(self):
        clients = [self.connect() for _ in range(10)]

        for i, client in enumerate(clients):
            self.assertEqual(client.command("SET", f"k{i}", f"v{i}"), b"+OK\r\n")

        # Read back in reverse order to force interleaving across connections.
        for i, client in reversed(list(enumerate(clients))):
            expected = f"v{i}".encode()
            self.assertEqual(
                client.command("GET", f"k{i}"),
                b"$%d\r\n%s\r\n" % (len(expected), expected),
            )

    def test_one_client_disconnecting_does_not_disturb_another(self):
        a = self.connect()
        b = self.connect()
        a.command("SET", "shared", "still_here")
        a.close()

        self.assertEqual(b.command("GET", "shared"), b"$10\r\nstill_here\r\n")
        self.assertEqual(b.command("PING"), b"+PONG\r\n")

    def test_malformed_input_closes_only_the_offending_client(self):
        a = self.connect()
        b = self.connect()

        a.send_raw(b"garbage\r\n")
        reply = a.read_reply()
        self.assertTrue(reply.startswith(b"-ERR Protocol error:"), reply)

        # B is untouched and the server is still accepting new connections.
        self.assertEqual(b.command("PING"), b"+PONG\r\n")
        self.assertEqual(self.connect().command("PING"), b"+PONG\r\n")


if __name__ == "__main__":
    unittest.main()
