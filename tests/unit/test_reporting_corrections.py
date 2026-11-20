import json
import unittest
from pathlib import Path

from full_etl_pipeline.corrections import RepairSQL, select_from_ctas
from full_etl_pipeline.daily_processing import scoped_gold_sql
from full_etl_pipeline.reporting import calendar_bounds

ROOT = Path(__file__).resolve().parents[2]


class RecordingCursor:
    def __init__(self):
        self.statements = []

    def execute(self, sql):
        self.statements.append(sql)


class ReportingCorrectionTests(unittest.TestCase):
    def test_calendar_is_explicit_full_year_and_bounded(self):
        self.assertEqual(("2020-01-01", "2035-12-31"), calendar_bounds({}))
        for cfg in ({"calendar_start": "2026-02-01"},
                    {"calendar_start": "2040-01-01"},
                    {"calendar_start": "1800-01-01", "calendar_end": "2100-12-31"}):
            with self.assertRaises(ValueError):
                calendar_bounds(cfg)

    def test_all_repair_work_is_dml_after_preparation(self):
        cur = RecordingCursor()
        runner = RepairSQL(cur, ROOT / "include/sql")
        paths = list((ROOT / "include/sql/silver").glob("validate_*.sql"))
        paths += list((ROOT / "include/sql/gold").glob("prepare_*.sql"))
        paths += [ROOT / "include/sql/gold/resolve_inventory_observations.sql"]
        for path in paths:
            layer = path.parent.name
            runner.prepare(layer, path.name)
            before = len(cur.statements)
            runner.run(layer, path.name)
            self.assertEqual(2, len(cur.statements) - before)
            self.assertTrue(cur.statements[-2].startswith("DELETE FROM"))
            self.assertTrue(cur.statements[-1].startswith("INSERT INTO"))
            self.assertNotIn("CREATE OR REPLACE", cur.statements[-1])

    def test_non_ctas_rejected(self):
        with self.assertRaises(ValueError):
            select_from_ctas("DELETE FROM example")

    def test_daily_inventory_publication_scoped_to_observation_date(self):
        sql = (ROOT / "include/sql/gold/merge_fct_inventory_daily.sql").read_text()
        scoped = scoped_gold_sql(sql, "merge_fct_inventory_daily.sql", "2026-11-09")
        self.assertIn("WHERE SNAPSHOT_DATE = '2026-11-09'::DATE", scoped)
        self.assertNotIn("EFFECTIVE_FROM", scoped)
        with self.assertRaises(RuntimeError):
            scoped_gold_sql(sql.replace("WORK_INV_OBSERVATIONS", "CHANGED"),
                            "merge_fct_inventory_daily.sql", "2026-11-09")

    def test_future_calendar_not_a_loaded_business_date(self):
        from full_etl_pipeline.daily_processing import latest_warehouse_date

        class Cursor(RecordingCursor):
            def fetchone(self):
                return (None,)

        cur = Cursor()
        latest_warehouse_date(cur, [])
        self.assertNotIn("DIM_DATE", cur.statements[0])

    def test_configuration_includes_calendar(self):
        cfg = json.loads((ROOT / "config/retail_gold_config.json").read_text())
        self.assertEqual(5, len(cfg["dimension_tables"]))
        self.assertIn("RETAIL_DWH.TGT.DIM_DATE", cfg["dimension_tables"])
        calendar_bounds(cfg)
