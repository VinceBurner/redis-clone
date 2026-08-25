"""A bare-bones, single-client, blocking TCP server speaking RESP.

Supports only PING for now. No threading, no async, no GET/SET/DEL yet.
"""

import socket

from resp import encode_error, encode_simple_string, parse_command_with_length

HOST = "localhost"
PORT = 6380


def dispatch(args: list[str]) -> bytes:
    command = args[0].upper()
    if command == "PING":
        return encode_simple_string("PONG")
    return encode_error(f"ERR unknown command '{args[0]}'")


def handle_client(conn: socket.socket) -> None:
    buffer = b""
    while True:
        try:
            chunk = conn.recv(4096)
        except ConnectionError:
            break

        if not chunk:
            break

        buffer += chunk

        while True:
            try:
                result = parse_command_with_length(buffer)
            except ValueError as exc:
                conn.sendall(encode_error(f"ERR Protocol error: {exc}"))
                return

            if result is None:
                break

            args, consumed = result
            buffer = buffer[consumed:]
            conn.sendall(dispatch(args))


def main() -> None:
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((HOST, PORT))
    server_socket.listen(1)
    print(f"redis-clone listening on {HOST}:{PORT}")

    try:
        while True:
            conn, addr = server_socket.accept()
            print(f"client connected: {addr}")
            try:
                handle_client(conn)
            finally:
                conn.close()
                print(f"client disconnected: {addr}")
    except KeyboardInterrupt:
        pass
    finally:
        server_socket.close()


if __name__ == "__main__":
    main()
