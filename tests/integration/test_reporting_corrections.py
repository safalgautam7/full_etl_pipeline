"""Actual repair DML/classifiers against DuckDB; NOT Snowflake certification."""

import csv
import hashlib
import json
import re
import unittest
from pathlib import Path

from full_etl_pipeline.corrections import correct_file
from full_etl_pipeline.silver_processing import SOURCES
import test_backfill_extension as extension
import test_scd_evidence as evidence

ROOT = Path(__file__).resolve().parents[2]


def reporting_sql(sql):
    sql = extension.local_sql(sql)
    sql = re.sub(r"TO_NUMBER\(TO_CHAR\(([^,]+), 'YYYYMMDD'\)\)",
                 r"CAST(strftime(\1, '%Y%m%d') AS BIGINT)", sql)
    sql = re.sub(r"OBJECT_CONSTRUCT_KEEP_NULL\(([a-z]+)\.\*\)", r"to_json(\1)", sql)
    sql = re.sub(r"DATEDIFF\('DAY', (.*?), (.*?)\)", r"date_diff('day', \1, \2)", sql)
    # DuckDB MERGE assignments cannot qualify the target column.
    if "UPDATE SET" in sql:
        before, update = sql.split("UPDATE SET", 1)
        sql = before + "UPDATE SET" + re.sub(r"\bt\.([A-Z_]+)\s*=", r"\1 =", update)
    return sql


class RepairCursor:
    def __init__(self, conn):
        self.conn = conn
        self.statements = []
        self.fail_on = None

    def execute(self, sql, params=None):
        self.statements.append(sql)
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("injected repair failure")
        self.conn.execute(reporting_sql(sql).replace("%s", "?"), params or [])
        return self

    def fetchone(self):
        return self.conn.fetchone()

    def fetchall(self):
        return self.conn.fetchall()


@unittest.skipIf(evidence.duckdb is None, "Install requirements-test.txt")
class ReportingRepairTests(unittest.TestCase):
    def setUp(self):
        extension.BackfillExtensionTests.setUp(self)
        self.conn.execute("SET threads=1")
        self.conn.execute("CREATE SCHEMA RETAIL_DWH.CONTROL")
        ddl = (ROOT / "include/sql/ddl/create_reporting_correction_tables.sql").read_text()
        ddl += (ROOT / "include/sql/control/01_file_manifest.sql").read_text()
        ddl = "\n".join(line for line in ddl.splitlines() if not line.lstrip().startswith("--"))
        for sql in ddl.split(";"):
            if sql.strip().startswith("CREATE TABLE"):
                self.conn.execute(sql.replace("TIMESTAMP_LTZ", "TIMESTAMPTZ").replace("VARIANT", "JSON")
                                  .replace("NUMBER(", "DECIMAL(").replace("CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP"))
        # Calendar shape is production DDL; populate dates locally. Attribute SQL
        # is exercised separately by the opt-in Snowflake acceptance test.
        self.conn.execute("""INSERT INTO RETAIL_DWH.TGT.DIM_DATE
            SELECT CAST(strftime(d, '%Y%m%d') AS BIGINT), d, year(d), quarter(d), month(d),
                   strftime(d, '%B'), year(d)*100+month(d), day(d), isodow(d), strftime(d, '%A'),
                   week(d), isoyear(d), date_trunc('month',d), last_day(d)
            FROM generate_series(DATE '2020-01-01',DATE '2035-12-31',INTERVAL 1 DAY) x(d)""")
        extension.BackfillExtensionTests.process_range(self, 9, 13)
        for source in SOURCES:
            self.conn.execute(f"""INSERT INTO RETAIL_DWH.CONTROL.FILE_MANIFEST
                (SOURCE_FILE_ID,SOURCE_FILE_NAME,SOURCE_FILE_SHA256,SOURCE_FILE_DATE,SOURCE_TYPE,STG_TABLE,
                 ROW_COUNT,LOAD_STATUS,BATCH_ID)
                SELECT SOURCE_FILE_ID,SOURCE_FILE_NAME,'placeholder',SOURCE_FILE_DATE,?, ?,COUNT(*),'LOADED','fixture'
                FROM RETAIL_DWH.STG.{source.upper()} GROUP BY 1,2,4""", [source, f"RETAIL_DWH.STG.{source.upper()}"])
        for path in (ROOT / "data").glob("*.csv"):
            self.conn.execute("UPDATE RETAIL_DWH.CONTROL.FILE_MANIFEST SET SOURCE_FILE_SHA256=? WHERE SOURCE_FILE_NAME=?",
                              [hashlib.sha256(path.read_bytes()).hexdigest(), path.name])
        self.repair_cursor = RepairCursor(self.conn)
        self.repair_cursor.execute((ROOT / "include/sql/gold/resolve_inventory_observations.sql").read_text())
        self.repair_cursor.execute((ROOT / "include/sql/gold/merge_fct_inventory_daily.sql").read_text())
        self.counter = 0

    run_gold = extension.BackfillExtensionTests.run_gold
    classify = extension.BackfillExtensionTests.classify
    ingest = extension.BackfillExtensionTests.ingest
    assert_reconciled = extension.BackfillExtensionTests.assert_reconciled

    def stage(self, filename, mutate):
        source = filename.rsplit("_", 1)[0]
        old_sha = self.conn.execute("SELECT SOURCE_FILE_SHA256 FROM RETAIL_DWH.CONTROL.FILE_MANIFEST "
                                    "WHERE SOURCE_FILE_NAME=?", [filename]).fetchone()[0]
        with (ROOT / "data" / filename).open(newline="") as stream:
            rows = list(csv.DictReader(stream))
        rows = mutate(rows)
        self.counter += 1
        new_sha = hashlib.sha256(json.dumps(rows).encode()).hexdigest()
        file_id = hashlib.sha256(f"{filename}:{new_sha}".encode()).hexdigest()
        self.conn.execute(f"CREATE OR REPLACE TABLE RETAIL_DWH.STG.REPLACEMENT_INPUT AS "
                          f"SELECT * FROM RETAIL_DWH.STG.{source.upper()} WHERE 1=0")
        for i, row in enumerate(rows, 1):
            self.conn.execute("INSERT INTO RETAIL_DWH.STG.REPLACEMENT_INPUT VALUES (?,?,?,CAST(? AS DATE),?,?,?)",
                              [hashlib.sha256(f"{file_id}:{i}".encode()).hexdigest(), file_id, filename,
                               f"{filename[-12:-8]}-{filename[-8:-6]}-{filename[-6:-4]}", i, "correction", json.dumps(row)])
        return dict(filename=filename, source=source, old_sha=old_sha, new_sha=new_sha,
                    new_file_id=file_id, correction_id=f"correction-{self.counter}",
                    reason="Approved test correction", approved_by="test operator")

    def repair(self, request):
        return correct_file(self.repair_cursor, self.cfg, ROOT / "include/sql", **request)

    def snapshot(self):
        tables = [f"RETAIL_DWH.{layer}.{source.upper()}" for layer in ("STG", "TMP") for source in SOURCES]
        tables += ["RETAIL_DWH.DQ.QUARANTINE_RECORDS"]
        tables += [f"RETAIL_DWH.TGT.{t}" for t in ("DIM_ITEM", "DIM_LOCN", "DIM_SUBCLASS", "DIM_DISTRICT",
                   "FCT_SALES", "FCT_INVENTORY", "FCT_INVENTORY_DAILY")]
        tables += [f"RETAIL_DWH.CONTROL.{t}" for t in ("FILE_MANIFEST", "RAW_VERSION_ARCHIVE", "MANIFEST_VERSION_ARCHIVE", "DQ_DECISION_ARCHIVE")]
        return {t: self.conn.execute(f"SELECT * FROM {t} ORDER BY ALL").fetchall() for t in tables}

    def test_sale_replacement_removes_lines_updates_totals_and_replays(self):
        before = self.conn.execute("SELECT COUNT(*), SUM(NET_SALES) FROM RETAIL_DWH.TGT.FCT_SALES").fetchone()
        def change(rows):
            rows[0]["discount_amount"] = "0.50"
            rows[0]["net_sales"] = str(float(rows[0]["quantity"]) * float(rows[0]["unit_price"]) - .5)
            return rows[:-1]
        request = self.stage("pos_sales_20261109.csv", change)
        self.assertEqual("SUCCEEDED", self.repair(request))
        after = self.conn.execute("SELECT COUNT(*), SUM(NET_SALES) FROM RETAIL_DWH.TGT.FCT_SALES").fetchone()
        self.assertNotEqual(before, after)
        snapshot = self.snapshot()
        request["correction_id"] = "replay"
        self.assertEqual("NOOP", self.repair(request))
        self.assertEqual(snapshot, self.snapshot())
        self.assertTrue(self.conn.execute("SELECT COUNT(*) FROM RETAIL_DWH.CONTROL.RAW_VERSION_ARCHIVE").fetchone()[0])
        # No DDL in any business publication transaction.
        in_transaction = False
        for sql in self.repair_cursor.statements:
            if sql == "BEGIN":
                in_transaction = True
            elif sql in ("COMMIT", "ROLLBACK"):
                in_transaction = False
            elif in_transaction:
                self.assertNotRegex(sql, r"(?i)\b(CREATE|ALTER|TRUNCATE)\b")

    def test_master_removal_revalidates_dependents_and_rebuilds_intervals(self):
        request = self.stage("item_master_20261109.csv", lambda rows: [r for r in rows if r["item_id"] != "ITEM001"])
        self.repair(request)
        self.assertEqual(0, self.conn.execute("SELECT COUNT(*) FROM RETAIL_DWH.TGT.FCT_SALES "
                                            "WHERE ITEM_ID='ITEM001' AND SALE_DATE='2026-11-09'").fetchone()[0])
        self.assertTrue(self.conn.execute("SELECT COUNT(*) FROM RETAIL_DWH.DQ.QUARANTINE_RECORDS "
                                         "WHERE REASON_CODE='UNKNOWN_ITEM_ID'").fetchone()[0])

    def test_inventory_change_and_removed_observation_repair_both_facts(self):
        def change(rows):
            rows[0]["quantity_on_hand"] = "123"
            rows[0]["inventory_value"] = str(123 * float(rows[0]["unit_cost"]))
            return rows[:-1]
        self.repair(self.stage("inventory_snapshot_20261110.csv", change))
        actual = self.conn.execute("SELECT COUNT(*),MAX(QUANTITY_ON_HAND) FROM RETAIL_DWH.TGT.FCT_INVENTORY_DAILY "
                                   "WHERE SNAPSHOT_DATE='2026-11-10'").fetchone()
        self.assertEqual((29, 123), actual)

    def test_failure_rolls_back_raw_silver_gold_manifest_and_archives(self):
        request = self.stage("pos_sales_20261109.csv", lambda rows: rows[:-1])
        before = self.snapshot()
        self.repair_cursor.fail_on = "MERGE INTO RETAIL_DWH.TGT.FCT_INVENTORY_DAILY"
        with self.assertRaisesRegex(RuntimeError, "injected repair failure"):
            self.repair(request)
        self.assertEqual(before, self.snapshot())
        self.assertEqual("FAILED", self.conn.execute("SELECT STATUS FROM RETAIL_DWH.CONTROL.CORRECTION_RUNS").fetchone()[0])

    def test_empty_sales_replacement_is_supported(self):
        self.repair(self.stage("pos_sales_20261109.csv", lambda rows: []))
        self.assertEqual(0, self.conn.execute("SELECT COUNT(*) FROM RETAIL_DWH.TGT.FCT_SALES "
                                            "WHERE SALE_DATE='2026-11-09'").fetchone()[0])

    def test_stale_approval_refused_without_business_changes(self):
        request = self.stage("pos_sales_20261109.csv", lambda rows: rows[:-1])
        request["old_sha"] = "0" * 64
        before = self.snapshot()
        with self.assertRaisesRegex(RuntimeError, "checksum"):
            self.repair(request)
        self.assertEqual(before, self.snapshot())
