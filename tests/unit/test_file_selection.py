"""Date-range discovery tests: no Airflow or Snowflake required."""

import unittest
from pathlib import Path

from full_etl_pipeline.file_selection import inspect_filename, select_files


class FileSelectionTests(unittest.TestCase):
    def setUp(self):
        self.names = [
            f"{source}_202611{day:02d}.csv"
            for source in ("item_master", "pos_sales", "inventory_snapshot")
            for day in range(9, 14)
        ] + ["store_master_20261109.csv", "store_master_20261112.csv"]

    def test_legacy_all_and_limit(self):
        self.assertEqual(select_files(self.names), (sorted(self.names), []))
        self.assertEqual(select_files(self.names, 1), (sorted(self.names)[:1], []))
        self.assertEqual(select_files(self.names + ["other.csv"])[0], sorted(self.names))

    def test_inclusive_range_and_dependencies(self):
        selected, dependencies = select_files(self.names, 0, "2026-11-10", "2026-11-12")
        self.assertEqual(dependencies, ["store_master_20261109.csv"])
        self.assertEqual(len(selected), 11)
        self.assertIn("pos_sales_20261110.csv", selected)
        self.assertIn("pos_sales_20261112.csv", selected)
        self.assertNotIn("pos_sales_20261109.csv", selected)
        self.assertNotIn("pos_sales_20261113.csv", selected)

    def test_single_day_latest_prior_masters(self):
        names = self.names + ["item_master_20261108.csv", "store_master_20261108.csv"]
        selected, dependencies = select_files(names, 0, "2026-11-11", "2026-11-11")
        self.assertEqual(len(selected), 4)
        self.assertEqual(dependencies, ["store_master_20261109.csv"])
        self.assertNotIn("store_master_20261112.csv", selected)

    def test_prior_item_dependency_and_no_future_master(self):
        names = ["item_master_20261109.csv", "store_master_20261112.csv", "pos_sales_20261110.csv"]
        self.assertEqual(
            select_files(names, 0, "2026-11-10", "2026-11-10"),
            (["item_master_20261109.csv", "pos_sales_20261110.csv"], ["item_master_20261109.csv"]),
        )

    def test_missing_days_are_allowed(self):
        names = [name for name in self.names if "20261109" not in name]
        selected, _ = select_files(names, 0, "2026-11-09", "2026-11-13")
        self.assertEqual(selected, sorted(names))

    def test_no_match_does_not_load_only_prior_dependencies(self):
        with self.assertRaisesRegex(ValueError, "in range"):
            select_files(self.names, 0, "2026-12-01", "2026-12-02")

    def test_invalid_parameters(self):
        cases = [
            (-1, None, None), (0, "2026-11-09", None),
            (0, None, "2026-11-13"), (0, "2026-11-13", "2026-11-09"),
            (0, "2026-02-30", "2026-11-13"), (0, "2026-1-09", "2026-11-13"),
            (0, "", ""), (0, 20261109, "2026-11-13"),
            (1, "2026-11-09", "2026-11-13"),
        ]
        for args in cases:
            with self.subTest(args=args), self.assertRaises(ValueError):
                select_files(self.names, *args)

    def test_invalid_filename_and_empty_input(self):
        for name in ("wrong.csv", "pos_sales_20260230.csv"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                inspect_filename(name)
        with self.assertRaisesRegex(ValueError, "No recognized"):
            select_files(["other.csv"])

    def test_repository_fixture(self):
        root = Path(__file__).resolve().parents[2] / "data"
        names = [path.name for path in root.glob("*.csv")]
        selected, dependencies = select_files(names, 0, "2026-11-10", "2026-11-13")
        self.assertEqual(len(selected), 14)
        self.assertEqual(dependencies, ["store_master_20261109.csv"])


if __name__ == "__main__":
    unittest.main()
