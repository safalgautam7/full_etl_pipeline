import copy
import json
import re
import unittest
from pathlib import Path

from full_etl_pipeline.silver_processing import (
    LINEAGE_COLS,
    SOURCES,
    load_config,
    validate_config,
)

ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = ROOT / "include/sql/silver"


class SilverContractTests(unittest.TestCase):
    def setUp(self):
        self.cfg = load_config(ROOT / "config/retail_tmp.json")

    def test_every_source_is_validated(self):
        for source in SOURCES:
            for key in ("stg", "tmp", "work"):
                with self.subTest(source=source, key=key):
                    cfg = copy.deepcopy(self.cfg)
                    cfg["sources"][source][key] = "BAD; DROP TABLE DATA"
                    with self.assertRaises(ValueError):
                        validate_config(cfg)

    def test_missing_source_is_rejected(self):
        for source in SOURCES:
            with self.subTest(source=source):
                cfg = copy.deepcopy(self.cfg)
                del cfg["sources"][source]
                with self.assertRaises(ValueError):
                    validate_config(cfg)

    def test_quarantine_and_connection_are_validated(self):
        for field, value in [("quarantine_table", "unsafe"), ("snowflake_conn_id", "other")]:
            cfg = copy.deepcopy(self.cfg)
            cfg[field] = value
            with self.assertRaises(ValueError):
                validate_config(cfg)

    def test_all_accepted_columns_are_compared_updated_and_inserted(self):
        for source, definition in SOURCES.items():
            with self.subTest(source=source):
                sql = (SQL_DIR / f"merge_{source}.sql").read_text()
                compared = set(re.findall(r"t\.(\w+) IS DISTINCT FROM s\.\1", sql))
                expected = set((LINEAGE_COLS + ", " + definition["cols"]).split(", ")) - {"SOURCE_ROW_ID"}
                self.assertEqual(expected, compared)
                updates = sql.split("THEN UPDATE SET", 1)[1].split("WHEN NOT MATCHED", 1)[0]
                for column in expected:
                    self.assertRegex(updates, rf"\b{column}\s*=\s*s\.{column}\b")
                self.assertNotRegex(updates, r"\bSOURCE_ROW_ID\s*=")
                self.assertNotIn("PROCESSED_AT", sql.split("WHEN MATCHED AND", 1)[1].split("THEN", 1)[0])
                insert = sql.split("WHEN NOT MATCHED THEN INSERT", 1)[1]
                for column in expected | {"SOURCE_ROW_ID"}:
                    self.assertIn(f"s.{column}", insert)

    def test_no_runtime_fixture_dependency(self):
        for name in ["silver_processing.py", "retail_tmp_processing.py", "retail_gold_pipeline.py"]:
            text = (ROOT / "dags/full_etl_pipeline" / name).read_text()
            self.assertNotIn("expected_results.json", text)

    def test_fixture_is_still_the_selected_17_files(self):
        import csv

        oracle = json.loads((ROOT / "tests/fixtures/expected_results.json").read_text())
        files = sorted(p for p in (ROOT / "data").glob("*.csv") if p.stem[-8:] <= "20261113")
        self.assertEqual(oracle["expected_total_files"], len(files))
        counts = {source: 0 for source in SOURCES}
        for path in files:
            with path.open(newline="", encoding="utf-8-sig") as stream:
                counts[path.name.rsplit("_", 1)[0]] += sum(1 for _ in csv.DictReader(stream))
        self.assertEqual(oracle["expected_stg_rows_by_source"], counts)


if __name__ == "__main__":
    unittest.main()
