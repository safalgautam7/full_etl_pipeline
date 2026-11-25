"""Reporting attributes and star views against local SQL, not live Snowflake."""

import re
import unittest
from pathlib import Path

from full_etl_pipeline.corrections import RepairSQL, repair_silver
import test_reporting_corrections as repair_tests
import test_scd_evidence as evidence

ROOT = Path(__file__).resolve().parents[2]


def mart_sql(sql):
    sql = repair_tests.reporting_sql(sql)
    sql = re.sub(r"TO_VARCHAR\(([^()]+)\)", r"CAST(\1 AS VARCHAR)", sql)
    sql = re.sub(r"TO_CHAR\(([^,]+), 'YYYY-MM'\)", r"strftime(\1, '%Y-%m')", sql)
    sql = re.sub(r"LAST_DAY\(([^,]+), 'QUARTER'\)",
                 r"CAST(date_trunc('quarter', \1) + INTERVAL 3 MONTH - INTERVAL 1 DAY AS DATE)", sql)
    sql = re.sub(r"DATEADD\('DAY', ([^,]+), ([^)]+)\)::DATE",
                 r"CAST(\2 + CAST(\1 AS INTEGER) AS DATE)", sql)
    return sql.replace("DAYOFWEEKISO", "isodow").replace("YEAROFWEEKISO", "isoyear").replace("WEEKISO", "week")


@unittest.skipIf(evidence.duckdb is None, "Install requirements-test.txt")
class SalesMartTests(unittest.TestCase):
    def setUp(self):
        repair_tests.ReportingRepairTests.setUp(self)
        self.cursor = repair_tests.RepairCursor(self.conn)

    run_gold = repair_tests.ReportingRepairTests.run_gold
    classify = repair_tests.ReportingRepairTests.classify
    ingest = repair_tests.ReportingRepairTests.ingest
    assert_reconciled = repair_tests.ReportingRepairTests.assert_reconciled

    def test_star_iso_dates_flattened_attributes_and_amounts(self):
        sql = (ROOT / "include/sql/ddl/03_create_sales_mart.sql").read_text()
        sql = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
        for statement in sql.split(";"):
            if statement.strip():
                self.conn.execute(mart_sql(statement))
        self.assertEqual("2025-01-02", str(self.conn.execute("SELECT PY_COMPARABLE_DATE FROM "
            "RETAIL_DWH.MART_SALES.DIM_DATE WHERE CALENDAR_DATE='2026-01-01'").fetchone()[0]))
        self.assertIsNone(self.conn.execute("SELECT PY_COMPARABLE_DATE FROM RETAIL_DWH.MART_SALES.DIM_DATE "
                                           "WHERE CALENDAR_DATE='2026-12-31'").fetchone()[0])
        self.assertEqual(145, self.conn.execute("SELECT COUNT(*) FROM RETAIL_DWH.MART_SALES.FCT_SALES").fetchone()[0])
        self.assertEqual(0, self.conn.execute("SELECT COUNT(*) FROM RETAIL_DWH.MART_SALES.FCT_SALES "
            "WHERE GROSS_SALES_AMOUNT-DISCOUNT_AMOUNT<>NET_SALES_AMOUNT").fetchone()[0])
        checks = self.conn.execute(mart_sql((ROOT / "include/sql/reconcile/check_sales_mart_integrity.sql").read_text())).fetchall()
        self.assertTrue(all(n == 0 for _, n in checks), checks)

    def test_missing_hierarchy_and_location_type_are_quarantined(self):
        self.conn.execute("UPDATE RETAIL_DWH.STG.ITEM_MASTER SET RAW_PAYLOAD="
                          "json_merge_patch(RAW_PAYLOAD, '{\"department_id\":null}') "
                          "WHERE SOURCE_FILE_NAME='item_master_20261109.csv' AND SOURCE_ROW_NUMBER=1")
        self.conn.execute("UPDATE RETAIL_DWH.STG.STORE_MASTER SET RAW_PAYLOAD="
                          "json_merge_patch(RAW_PAYLOAD, '{\"location_type\":null}') "
                          "WHERE SOURCE_FILE_NAME='store_master_20261109.csv' AND SOURCE_ROW_NUMBER=1")
        runner = RepairSQL(self.cursor, ROOT / "include/sql")
        from full_etl_pipeline.silver_processing import SOURCES
        for definition in SOURCES.values():
            runner.prepare("silver", definition["sql"])
        self.conn.execute("BEGIN")
        repair_silver(self.cursor, self.cfg, runner)
        self.conn.execute("COMMIT")
        reasons = {r[0] for r in self.conn.execute("SELECT REASON_CODE FROM RETAIL_DWH.DQ.QUARANTINE_RECORDS").fetchall()}
        self.assertIn("INCOMPLETE_ITEM_HIERARCHY", reasons)
        self.assertIn("NULL_LOCATION_TYPE", reasons)
