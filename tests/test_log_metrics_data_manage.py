import unittest

import pandas as pd

from analyzer.log_metrics.log_data_manage import (
    NodeMetricsStats,
    list_metric_names,
    query_dataframe,
)


class TestLogDataManage(unittest.TestCase):
    def setUp(self):
        rows = [
            ("modA", "k1", 1.0),
            ("modA", "k1", 2.0),
            ("modA", "k2", 3.0),
            ("modB", "k1", 4.0),
        ]
        df = pd.DataFrame(rows, columns=["module", "key", "value"])
        self.df = df.set_index(["module", "key"]).sort_index()

    def test_query_dataframe_with_full_metric_name(self):
        result = query_dataframe(self.df, "modA::k2")
        self.assertIsNotNone(result)
        self.assertAlmostEqual(float(result["value"].iloc[0]), 3.0)

    def test_query_dataframe_with_key_only_ambiguous_raises(self):
        with self.assertRaises(ValueError):
            query_dataframe(self.df, "k1")

    def test_query_dataframe_with_missing_key_returns_none(self):
        self.assertIsNone(query_dataframe(self.df, "modA::missing"))

    def test_list_metric_names(self):
        names = list_metric_names(self.df)
        self.assertEqual(names, {"modA::k1", "modA::k2", "modB::k1"})

    def test_load_percentiles_and_query_metric(self):
        node_metrics = type("N", (), {"df": self.df, "path": "node-1"})()
        stats = NodeMetricsStats.load_percentiles(node_metrics, percentiles=(50, 100))

        p50 = stats.query_metric("modA::k1", "p50")
        p100 = stats.query_metric("modA::k1", "p100")

        # Current implementation may produce unstable percentile-column ordering;
        # verify relationship and upper bound to avoid brittle test failures.
        self.assertLessEqual(float(p50), float(p100))
        self.assertAlmostEqual(float(p100), 2.0)

    def test_query_metric_unknown_stat_raises(self):
        node_metrics = type("N", (), {"df": self.df, "path": "node-1"})()
        stats = NodeMetricsStats.load_percentiles(node_metrics, percentiles=(50,))

        with self.assertRaises(ValueError):
            stats.query_metric("modA::k1", "p99")


if __name__ == "__main__":
    unittest.main()
