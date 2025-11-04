import unittest
from datetime import datetime, timezone

from procplan.timeutils import ensure_hour_alignment, hour_range, parse_iso_timestamp


class TimeUtilsTests(unittest.TestCase):
    def test_parse_iso_timestamp_assigns_utc(self):
        naive = "2024-05-01T12:00:00"
        aware = parse_iso_timestamp(naive)
        self.assertEqual(aware.tzinfo, timezone.utc)
        self.assertEqual(aware.hour, 12)

    def test_parse_iso_timestamp_converts_to_utc(self):
        value = "2024-05-01T14:00:00+02:00"
        converted = parse_iso_timestamp(value)
        self.assertEqual(converted.tzinfo, timezone.utc)
        self.assertEqual(converted.hour, 12)

    def test_hour_range_returns_inclusive_exclusive_hours(self):
        start = datetime(2024, 5, 1, 0, 0, tzinfo=timezone.utc)
        end = datetime(2024, 5, 1, 3, 0, tzinfo=timezone.utc)
        hours = hour_range(start, end)
        self.assertEqual(len(hours), 3)
        self.assertEqual(hours[0], start)
        self.assertEqual(hours[-1], datetime(2024, 5, 1, 2, 0, tzinfo=timezone.utc))

    def test_hour_range_empty_when_end_before_or_equal_start(self):
        start = datetime(2024, 5, 1, 0, 0, tzinfo=timezone.utc)
        end = datetime(2024, 5, 1, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(hour_range(start, end), [])

    def test_ensure_hour_alignment_raises_on_minute(self):
        bad = datetime(2024, 5, 1, 1, 30, tzinfo=timezone.utc)
        with self.assertRaises(ValueError):
            ensure_hour_alignment(bad)


if __name__ == "__main__":
    unittest.main()
