"""Native Airflow date scope; no API connection or warehouse required."""

from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
import hashlib
import json
import unittest

from full_etl_pipeline.daily_processing import processing_day, select_day_files, scoped_gold_sql, verify_replay_files

ROOT = Path(__file__).resolve().parents[2]


class NativeBackfillTests(unittest.TestCase):
    def context(self, run_type, dag_id="retail_full_etl"):
        return {"dag": SimpleNamespace(dag_id=dag_id),
                "dag_run": SimpleNamespace(run_type=run_type),
                "data_interval_start": datetime(2026, 11, 9, tzinfo=timezone.utc)}

    def test_native_backfill_and_scheduled_use_interval_date(self):
        for kind in ("backfill", "scheduled"):
            self.assertEqual(processing_day(self.context(kind)), "2026-11-09")
        class Kind(Enum):
            BACKFILL = "backfill"
        self.assertEqual(processing_day(self.context(Kind.BACKFILL)), "2026-11-09")

    def test_manual_and_standalone_keep_original_behavior(self):
        self.assertIsNone(processing_day(self.context("manual")))
        self.assertIsNone(processing_day(self.context("backfill", "retail_stg_ingestion")))

    def test_each_fixture_date_gets_only_its_own_files(self):
        names = [p.name for p in (ROOT / "data").glob("*.csv")]
        counts = []
        for day in range(9, 19):
            selected = select_day_files(names, f"2026-11-{day:02d}")
            counts.append(len(selected))
            self.assertTrue(all(f"202611{day:02d}" in name for name in selected))
        self.assertEqual(counts, [4, 3, 4, 3, 3, 3, 4, 3, 3, 3])

    def test_missing_daily_inputs_fail(self):
        with self.assertRaisesRegex(ValueError, "missing required daily inputs"):
            select_day_files(["item_master_20261109.csv"], "2026-11-09")

    def test_gold_publication_is_daily_with_prior_interval_closure(self):
        for filename in ("load_dim_subclass.sql", "load_dim_district.sql", "prepare_fct_sales.sql",
                         "merge_dim_item.sql", "merge_dim_locn.sql", "merge_fct_inventory.sql"):
            sql = (ROOT / "include/sql/gold" / filename).read_text()
            sql = scoped_gold_sql(sql, filename, "2026-11-09")
            self.assertIn("'2026-11-09'::DATE", sql)
            if filename.startswith("merge_"):
                self.assertIn("OR EFFECTIVE_TO =", sql)

    def test_scd1_replay_keeps_latest_description_for_todays_keys(self):
        path = ROOT / "include/sql/gold/load_dim_subclass.sql"
        sql = scoped_gold_sql(path.read_text(), path.name, "2026-11-09")
        self.assertIn("SUBCLASS_ID IN (SELECT SUBCLASS_ID", sql)
        self.assertIn("ORDER BY SNAPSHOT_DATE DESC", sql)

    def test_identical_bulk_loaded_file_is_replayable(self):
        cfg = json.loads((ROOT / "config/retail_etl.json").read_text())
        filename, digest = "pos_sales_20261109.csv", "a" * 64
        file_id = hashlib.sha256(f"{filename}:{digest}".encode()).hexdigest()
        cursor = Mock()
        cursor.fetchall.return_value = [(digest, 30, "LOADED", file_id)]
        cursor.fetchone.return_value = (30, 30)
        verify_replay_files(cursor, cfg, [(filename, digest)])
        self.assertTrue(all(c.args[0].startswith("SELECT") for c in cursor.execute.call_args_list))

    def test_changed_unregistered_or_incomplete_history_is_rejected(self):
        cfg = json.loads((ROOT / "config/retail_etl.json").read_text())
        filename, digest = "pos_sales_20261109.csv", "a" * 64
        file_id = hashlib.sha256(f"{filename}:{digest}".encode()).hexdigest()
        for manifest, counts in (([], (0, 0)), ([("changed", 30, "LOADED", file_id)], (30, 30)),
                                 ([(digest, 30, "LOADED", file_id)], (29, 29))):
            with self.subTest(manifest=manifest):
                cursor = Mock()
                cursor.fetchall.return_value = manifest
                cursor.fetchone.return_value = counts
                with self.assertRaises(RuntimeError):
                    verify_replay_files(cursor, cfg, [(filename, digest)])


if __name__ == "__main__":
    unittest.main()
