"""A single-threaded, event-driven TCP server speaking RESP.

Mirrors real Redis's architecture: one thread, one event loop, non-blocking
sockets multiplexed with `selectors`. Because only this thread ever touches
the DataStore, no locking is needed anywhere — and commands are atomic by
construction, since one command always runs to completion before the loop
looks at another client.

Run it with:  python3 -m redis_clone.server
"""

from __future__ import annotations

import math
import selectors
import socket

from .resp import (
    encode_bulk_string,
    encode_error,
    encode_integer,
    encode_simple_string,
    parse_command_with_length,
)
from .store import DataStore

HOST = "localhost"
PORT = 6380
READ_SIZE = 4096

# select() wakes this often even when idle, so a stop() issued from another
# thread (as the integration tests do) is noticed promptly.
SELECT_TIMEOUT = 0.1


class Connection:
    """Per-client state: the socket plus its pending input/output buffers.

    Both buffers are needed because non-blocking sockets deliver and accept
    partial data: a command can arrive split across several reads, and a
    reply can be accepted by the kernel only in part.
    """

    def __init__(self, sock: socket.socket, addr) -> None:
        self.sock = sock
        self.addr = addr
        self.inbox = b""
        self.outbox = b""
        self.closing = False  # close once outbox has drained
        self.closed = False


class Server:
    def __init__(self, host: str = HOST, port: int = PORT) -> None:
        self.host = host
        self.port = port
        self.store = DataStore()
        self._selector = selectors.DefaultSelector()
        self._server_socket: socket.socket | None = None
        self._running = False

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        """Bind and listen. Sets self.port when port 0 was requested."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.host, self.port))
        sock.listen()
        sock.setblocking(False)

        self.port = sock.getsockname()[1]
        self._server_socket = sock
        # data=None marks the listening socket; clients carry a Connection.
        self._selector.register(sock, selectors.EVENT_READ, data=None)

    def serve_forever(self) -> None:
        self._running = True
        try:
            while self._running:
                for key, mask in self._selector.select(timeout=SELECT_TIMEOUT):
                    if key.data is None:
                        self._accept()
                    else:
                        self._service(key.data, mask)
        finally:
            self._shutdown()

    def stop(self) -> None:
        """Ask the loop to exit. Safe to call from another thread."""
        self._running = False

    def _shutdown(self) -> None:
        for key in list(self._selector.get_map().values()):
            if key.data is not None:
                self._close(key.data)
        if self._server_socket is not None:
            self._selector.unregister(self._server_socket)
            self._server_socket.close()
            self._server_socket = None
        self._selector.close()

    # -- event handling ----------------------------------------------------

    def _accept(self) -> None:
        assert self._server_socket is not None
        try:
            sock, addr = self._server_socket.accept()
        except BlockingIOError:
            return
        sock.setblocking(False)
        conn = Connection(sock, addr)
        self._selector.register(sock, selectors.EVENT_READ, data=conn)

    def _service(self, conn: Connection, mask: int) -> None:
        if mask & selectors.EVENT_READ:
            self._on_readable(conn)
        if not conn.closed and mask & selectors.EVENT_WRITE:
            self._flush(conn)

    def _on_readable(self, conn: Connection) -> None:
        try:
            chunk = conn.sock.recv(READ_SIZE)
        except BlockingIOError:
            return
        except OSError:
            self._close(conn)
            return

        if not chunk:  # clean client disconnect
            self._close(conn)
            return

        conn.inbox += chunk

        while True:
            try:
                result = parse_command_with_length(conn.inbox)
            except ValueError as exc:
                # Malformed (not merely incomplete) input: reply, then hang
                # up on this client only — the loop keeps serving everyone.
                conn.outbox += encode_error(f"ERR Protocol error: {exc}")
                conn.closing = True
                break

            if result is None:  # need more bytes
                break

            args, consumed = result
            conn.inbox = conn.inbox[consumed:]
            conn.outbox += self.dispatch(args)

        self._flush(conn)

    def _flush(self, conn: Connection) -> None:
        """Send what we can, then register interest in whatever is left."""
        while conn.outbox:
            try:
                sent = conn.sock.send(conn.outbox)
            except BlockingIOError:
                break  # kernel buffer full; wait for EVENT_WRITE
            except OSError:
                self._close(conn)
                return
            conn.outbox = conn.outbox[sent:]

        if conn.outbox:
            self._set_interest(conn, selectors.EVENT_READ | selectors.EVENT_WRITE)
        elif conn.closing:
            self._close(conn)
        else:
            self._set_interest(conn, selectors.EVENT_READ)

    def _set_interest(self, conn: Connection, events: int) -> None:
        if self._selector.get_key(conn.sock).events != events:
            self._selector.modify(conn.sock, events, data=conn)

    def _close(self, conn: Connection) -> None:
        if conn.closed:
            return
        conn.closed = True
        try:
            self._selector.unregister(conn.sock)
        except KeyError:
            pass
        conn.sock.close()

    # -- commands ----------------------------------------------------------

    def dispatch(self, args: list[str]) -> bytes:
        command = args[0].upper()

        if command == "PING":
            return encode_simple_string("PONG")

        if command == "GET":
            if len(args) != 2:
                return self._arity_error("get")
            return encode_bulk_string(self.store.get(args[1]))

        if command == "SET":
            if len(args) != 3:
                return self._arity_error("set")
            self.store.set(args[1], args[2])
            return encode_simple_string("OK")

        if command == "DEL":
            if len(args) != 2:
                return self._arity_error("del")
            return encode_integer(1 if self.store.delete(args[1]) else 0)

        if command in ("INCR", "DECR"):
            if len(args) != 2:
                return self._arity_error(command.lower())
            delta = 1 if command == "INCR" else -1
            try:
                return encode_integer(self.store.increment(args[1], delta))
            except ValueError as exc:
                # Non-integer stored value: report it and keep the
                # connection open, same as any other command error.
                return encode_error(f"ERR {exc}")

        if command == "EXPIRE":
            if len(args) != 3:
                return self._arity_error("expire")
            try:
                seconds = float(args[2])
            except ValueError:
                return self._not_a_number_error()
            # NaN would compare False against every deadline and so never
            # expire; inf would never expire either. Reject both.
            if not math.isfinite(seconds):
                return self._not_a_number_error()
            return encode_integer(1 if self.store.expire(args[1], seconds) else 0)

        if command == "TTL":
            if len(args) != 2:
                return self._arity_error("ttl")
            return encode_integer(self.store.ttl(args[1]))

        return encode_error(f"ERR unknown command '{args[0]}'")

    @staticmethod
    def _arity_error(command: str) -> bytes:
        return encode_error(
            f"ERR wrong number of arguments for '{command}' command"
        )

    @staticmethod
    def _not_a_number_error() -> bytes:
        return encode_error("ERR value is not an integer or out of range")


def main() -> None:
    server = Server()
    server.start()
    print(f"redis-clone listening on {server.host}:{server.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.stop()


if __name__ == "__main__":
    main()
