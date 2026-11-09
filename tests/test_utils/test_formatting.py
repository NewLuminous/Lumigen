import unittest
from utils.formatting import (
    format_seconds_timestamp,
    format_nanosecond_timestamp,
)


class TestFormatting(unittest.TestCase):
    def test_format_seconds_timestamp_example(self):
        result = format_seconds_timestamp(1749124481)
        self.assertEqual(result, "2025-06-05T11:54:41.000000000Z")

    def test_format_seconds_timestamp_epoch(self):
        result = format_seconds_timestamp(0)
        self.assertEqual(result, "1970-01-01T00:00:00.000000000Z")

    def test_format_seconds_timestamp_y2k(self):
        # 2000-01-01T00:00:00Z
        result = format_seconds_timestamp(946684800)
        self.assertEqual(result, "2000-01-01T00:00:00.000000000Z")

    def test_format_nanosecond_timestamp_example(self):
        result = format_nanosecond_timestamp(1749124481528122919)
        self.assertEqual(result, "2025-06-05T11:54:41.528122919Z")

    def test_format_nanosecond_timestamp_zero_nanos(self):
        result = format_nanosecond_timestamp(1749124481000000000)
        self.assertEqual(result, "2025-06-05T11:54:41.000000000Z")

    def test_format_nanosecond_timestamp_padding(self):
        # 42 nanoseconds -> should be padded to 000000042
        base_sec = 1749124481
        ns = base_sec * 1_000_000_000 + 42
        result = format_nanosecond_timestamp(ns)
        self.assertEqual(result, "2025-06-05T11:54:41.000000042Z")

    def test_format_nanosecond_timestamp_max_nanos(self):
        base_sec = 1749124481
        ns = base_sec * 1_000_000_000 + 999_999_999
        result = format_nanosecond_timestamp(ns)
        self.assertEqual(result, "2025-06-05T11:54:41.999999999Z")


if __name__ == "__main__":
    unittest.main()

