import unittest
from datetime import datetime

import numpy as np

from analyzer.log_metrics.utils import (
    create_time_mask,
    parse_metric_name,
    sanitize_metric_name,
    trim_time_window,
)


class TestLogMetricsUtils(unittest.TestCase):
    def test_parse_metric_name_with_module(self):
        module, key = parse_metric_name("consensus::tx.count")
        self.assertEqual(module, "consensus")
        self.assertEqual(key, "tx.count")

    def test_parse_metric_name_without_module(self):
        module, key = parse_metric_name("tx.count")
        self.assertIsNone(module)
        self.assertEqual(key, "tx.count")

    def test_trim_time_window_prefix_and_suffix(self):
        timestamps = np.array([0, 60_000, 120_000, 180_000, 240_000], dtype=np.int64)
        values = np.array([1, 2, 3, 4, 5], dtype=np.float64)

        ts, vs = trim_time_window(timestamps, values, prefix_minutes=1, suffix_minutes=1)

        np.testing.assert_array_equal(ts, np.array([60_000, 120_000, 180_000], dtype=np.int64))
        np.testing.assert_array_equal(vs, np.array([2, 3, 4], dtype=np.float64))

    def test_create_time_mask_same_day_range(self):
        # 2025-01-01 10:00, 10:30, 11:00 (local time)
        timestamps = np.array([
            1735696800000,
            1735698600000,
            1735700400000,
        ], dtype=np.int64)

        mask = create_time_mask("10:15-10:45", timestamps)
        np.testing.assert_array_equal(mask, np.array([False, True, False]))

    def test_create_time_mask_cross_midnight(self):
        # Local time: 23:30, 00:10, 01:00
        timestamps = np.array([
            int(datetime(2025, 1, 1, 23, 30).timestamp() * 1000),
            int(datetime(2025, 1, 2, 0, 10).timestamp() * 1000),
            int(datetime(2025, 1, 2, 1, 0).timestamp() * 1000),
        ], dtype=np.int64)

        mask = create_time_mask("23:00-00:30", timestamps)
        np.testing.assert_array_equal(mask, np.array([True, True, False]))

    def test_sanitize_metric_name(self):
        self.assertEqual(sanitize_metric_name("a.b-c"), "a_b_c")
        self.assertEqual(sanitize_metric_name("123abc"), "_123abc")
        self.assertEqual(sanitize_metric_name(""), "_")


if __name__ == "__main__":
    unittest.main()
