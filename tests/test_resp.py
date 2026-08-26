import unittest

from redis_clone.resp import (
    encode_bulk_string,
    encode_error,
    encode_integer,
    encode_simple_string,
    parse_command,
)


class ParseCommandTests(unittest.TestCase):
    def test_parses_well_formed_ping(self):
        self.assertEqual(parse_command(b"*1\r\n$4\r\nPING\r\n"), ["PING"])

    def test_incomplete_data_returns_none(self):
        full = b"*1\r\n$4\r\nPING\r\n"
        for cut in range(len(full)):
            with self.subTest(cut=cut):
                self.assertIsNone(parse_command(full[:cut]))

    def test_empty_bytes_returns_none(self):
        self.assertIsNone(parse_command(b""))

    def test_parses_two_argument_command(self):
        data = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"
        self.assertEqual(parse_command(data), ["GET", "foo"])

    def test_parses_three_argument_command(self):
        data = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
        self.assertEqual(parse_command(data), ["SET", "foo", "bar"])

    def test_parses_single_zero_length_argument(self):
        data = b"*1\r\n$0\r\n\r\n"
        self.assertEqual(parse_command(data), [""])

    def test_malformed_input_raises(self):
        with self.assertRaises(ValueError):
            parse_command(b"garbage\r\n")


class EncodeSimpleStringTests(unittest.TestCase):
    def test_encodes_pong(self):
        self.assertEqual(encode_simple_string("PONG"), b"+PONG\r\n")

    def test_encodes_ok(self):
        self.assertEqual(encode_simple_string("OK"), b"+OK\r\n")


class EncodeErrorTests(unittest.TestCase):
    def test_encodes_error_message(self):
        self.assertEqual(
            encode_error("ERR unknown command"),
            b"-ERR unknown command\r\n",
        )


class EncodeIntegerTests(unittest.TestCase):
    def test_encodes_one(self):
        self.assertEqual(encode_integer(1), b":1\r\n")

    def test_encodes_zero(self):
        self.assertEqual(encode_integer(0), b":0\r\n")

    def test_encodes_negative(self):
        self.assertEqual(encode_integer(-1), b":-1\r\n")

    def test_encodes_large_value(self):
        self.assertEqual(encode_integer(1234567890), b":1234567890\r\n")


class EncodeBulkStringTests(unittest.TestCase):
    def test_encodes_string(self):
        self.assertEqual(encode_bulk_string("hello"), b"$5\r\nhello\r\n")

    def test_encodes_empty_string(self):
        self.assertEqual(encode_bulk_string(""), b"$0\r\n\r\n")

    def test_encodes_none_as_null_bulk_string(self):
        self.assertEqual(encode_bulk_string(None), b"$-1\r\n")


if __name__ == "__main__":
    unittest.main()
