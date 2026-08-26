"""RESP (REdis Serialization Protocol) parsing and encoding."""

from __future__ import annotations


def parse_command(data: bytes) -> list[str] | None:
    """Parse a RESP Array of Bulk Strings from `data`.

    Returns the list of decoded string arguments, or None if `data` does
    not yet contain a complete command (the caller should read more bytes
    and try again).
    """
    result = parse_command_with_length(data)
    if result is None:
        return None
    args, _consumed = result
    return args


def parse_command_with_length(data: bytes) -> tuple[list[str], int] | None:
    """Like parse_command, but also returns how many bytes were consumed.

    Used by the server to advance its read buffer past a parsed command
    so a second, pipelined command can still be parsed from the remainder.
    """
    if not data:
        return None

    if not data.startswith(b"*"):
        raise ValueError("expected RESP array (line must start with '*')")

    header_end = data.find(b"\r\n")
    if header_end == -1:
        return None

    count = int(data[1:header_end])
    pos = header_end + 2

    args: list[str] = []
    for _ in range(count):
        if not data[pos:pos + 1] == b"$":
            if pos >= len(data):
                return None
            raise ValueError("expected bulk string (line must start with '$')")

        len_end = data.find(b"\r\n", pos)
        if len_end == -1:
            return None

        length = int(data[pos + 1:len_end])
        value_start = len_end + 2
        value_end = value_start + length

        if len(data) < value_end + 2:
            return None

        args.append(data[value_start:value_end].decode())
        pos = value_end + 2

    return args, pos


def encode_simple_string(s: str) -> bytes:
    return f"+{s}\r\n".encode()


def encode_error(msg: str) -> bytes:
    return f"-{msg}\r\n".encode()


def encode_integer(n: int) -> bytes:
    return f":{n}\r\n".encode()


def encode_bulk_string(s: str | None) -> bytes:
    if s is None:
        return b"$-1\r\n"
    encoded = s.encode()
    return f"${len(encoded)}\r\n".encode() + encoded + b"\r\n"
