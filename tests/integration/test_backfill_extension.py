"""Execute checked-in classifiers, publisher, Gold and reconciliation locally.

Only Snowflake syntax is adapted for DuckDB. This is not certification of
Snowflake casts, PUT/COPY, transactions or native Airflow execution.
"""

import ast
import csv
import hashlib
import json
import re
import tempfile
import unittest
from datetime import date
from pathlib import Path

from full_etl_pipeline.daily_processing import select_day_files, verify_replay_decisions
from full_etl_pipeline.silver_processing import SOURCES, load_config, publish_work
import test_daily_gold as daily_gold
from test_silver_publication import Cursor
import test_scd_evidence as evidence

ROOT = Path(__file__).resolve().parents[2]


def local_sql(sql):
    """Narrow test-only dialect conversion; preserve validation CASE logic."""
    sql = sql.replace("TEMPORARY TABLE", "TABLE")
    sql = re.sub(r"([sp]\.)?RAW_PAYLOAD:([a-z_]+)::VARCHAR", r"json_extract_string(\1RAW_PAYLOAD, '$.\2')", sql)
    sql = re.sub(r"TRY_TO_DATE\((NULLIF\(TRIM\(.*?\), ''\)), 'YYYY-MM-DD'\)", r"TRY_CAST(\1 AS DATE)", sql)
    sql = re.sub(r"TRY_TO_(?:DECIMAL|NUMBER)\((NULLIF\(TRIM\(.*?\), ''\)), (\d+), (\d+)\)", r"TRY_CAST(\1 AS DECIMAL(\2,\3))", sql)
    sql = re.sub(r"TRY_TO_DECIMAL\((json_extract_string\(RAW_PAYLOAD, '\$\.[a-z_]+'\)), 18, 2\)", r"TRY_CAST(\1 AS DECIMAL(18,2))", sql)
    return sql.replace("REGEXP_LIKE", "regexp_full_match").replace("CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP")


@unittest.skipIf(evidence.duckdb is None, "Install requirements-test.txt")
class BackfillExtensionTests(unittest.TestCase):
    run_gold = daily_gold.DailyGoldTests.run_gold
    snapshot = daily_gold.DailyGoldTests.snapshot

    def setUp(self):
        # Reuse the existing Gold test schema, but load all fixtures through
        # actual Silver classifiers, not its original accepted-input shortcuts.
        daily_gold.DailyGoldTests.setUp(self)
        self.conn.execute("CREATE SCHEMA RETAIL_DWH.STG")
        self.conn.execute("CREATE SCHEMA RETAIL_DWH.DQ")
        self.conn.execute("CREATE MACRO REGEXP_LIKE(s, p) AS regexp_full_match(s, p)")
        for source in SOURCES:
            self.conn.execute(f"DROP TABLE RETAIL_DWH.TMP.{source.upper()}")
            self.conn.execute(f"""CREATE TABLE RETAIL_DWH.STG.{source.upper()} (
                SOURCE_ROW_ID VARCHAR, SOURCE_FILE_ID VARCHAR, SOURCE_FILE_NAME VARCHAR,
                SOURCE_FILE_DATE DATE, SOURCE_ROW_NUMBER BIGINT, BATCH_ID VARCHAR, RAW_PAYLOAD JSON)""")
        ddl = (ROOT / "include/sql/ddl/create_tmp_dq_tables.sql").read_text()
        ddl = "\n".join(line for line in ddl.splitlines() if not line.lstrip().startswith("--"))
        for statement in ddl.split(";"):
            if statement.strip().startswith("CREATE TABLE"):
                self.conn.execute(statement.replace("TIMESTAMP_LTZ", "TIMESTAMPTZ").replace("VARIANT", "JSON")
                                  .replace("NUMBER(", "DECIMAL(").replace("CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP"))
        self.cfg = load_config(ROOT / "config/retail_tmp.json")
        self.cursor = Cursor(self.conn)
        self.expected = json.loads((ROOT / "tests/fixtures/backfill_extension_expected.json").read_text())
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.sql_dir = Path(self.temp.name)
        for path in (ROOT / "include/sql/silver").glob("*.sql"):
            (self.sql_dir / path.name).write_text(local_sql(path.read_text()))

    def ingest(self, path):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        file_id = hashlib.sha256(f"{path.name}:{digest}".encode()).hexdigest()
        source = path.name.rsplit("_", 1)[0]
        day = date.fromisoformat(f"{path.stem[-8:-4]}-{path.stem[-4:-2]}-{path.stem[-2:]}")
        with path.open(newline="") as stream:
            rows = [(hashlib.sha256(f"{file_id}:{i}".encode()).hexdigest(), file_id, path.name,
                     day, i, "fixture-test", json.dumps(row))
                    for i, row in enumerate(csv.DictReader(stream), 1)]
        self.conn.executemany(f"INSERT INTO RETAIL_DWH.STG.{source.upper()} VALUES (?,?,?,?,?,?,?)", rows)

    def classify(self, source, day):
        names = self.cfg["sources"][source]
        scoped = f"RETAIL_DWH.TMP.TEST_RAW_{source.upper()}"
        self.conn.execute(f"CREATE OR REPLACE TABLE {scoped} AS SELECT * FROM {names['stg']} WHERE SOURCE_FILE_DATE=?", [day])
        if not self.conn.execute(f"SELECT COUNT(*) FROM {scoped}").fetchone()[0]:
            return
        sql = (self.sql_dir / SOURCES[source]["sql"]).read_text()
        sql = sql.replace(f"FROM RETAIL_DWH.STG.{source.upper()} s", f"FROM {scoped} s")
        if source == "pos_sales":
            sql = sql.replace("WHEN BUSINESS_KEY_RN > 1 THEN", f"""WHEN BUSINESS_KEY_RN > 1 OR EXISTS (
                SELECT 1 FROM {names['tmp']} previous WHERE previous.TRANSACTION_ID=r.TRANSACTION_ID
                AND previous.LINE_NUMBER=r.LINE_NUMBER AND previous.SALE_DATE < '{day}'::DATE) THEN""")
        self.conn.execute(sql)
        filename = f"{source}_{day.replace('-', '')}.csv"
        if filename in self.expected:
            expected = self.expected[filename]
            actual = self.conn.execute(f"SELECT SOURCE_ROW_NUMBER, REJECT_REASON FROM {names['work']} ORDER BY SOURCE_ROW_NUMBER").fetchall()
            self.assertEqual([(c["row"], c["reason"]) for c in expected["cases"]], actual, filename)
        cfg = json.loads(json.dumps(self.cfg))
        cfg["sources"][source]["stg"] = scoped
        publish_work(self.cursor, cfg, source, self.sql_dir)

    def process_range(self, start=9, end=18):
        paths = {p.name: p for p in (ROOT / "data").glob("*.csv")}
        for number in range(start, end + 1):
            day = f"2026-11-{number:02d}"
            selected = select_day_files(paths, day)
            for filename in selected:
                self.ingest(paths[filename])
            for source in SOURCES:
                self.classify(source, day)
            self.run_gold(day)
            self.assert_reconciled()

    def assert_reconciled(self):
        sql = local_sql((ROOT / "include/sql/reconcile/reconcile_gold.sql").read_text())
        rows = self.conn.execute(sql).fetchall()
        self.assertTrue(rows)
        self.assertTrue(all(row[-1] == "PASS" for row in rows), rows)
        integrity = self.conn.execute((ROOT / "include/sql/reconcile/check_gold_integrity.sql").read_text()).fetchall()
        self.assertTrue(all(count == 0 for _, count in integrity), integrity)

    def test_ten_days_classification_gold_reconciliation_and_overlapping_replay(self):
        self.process_range()
        self.assertEqual(33, self.conn.execute("SELECT COUNT(DISTINCT SOURCE_FILE_NAME) FROM (" +
                         " UNION ALL ".join(f"SELECT SOURCE_FILE_NAME FROM RETAIL_DWH.STG.{s.upper()}" for s in SOURCES) + ")").fetchone()[0])
        self.assertEqual({"DIM_SUBCLASS": 6, "DIM_DISTRICT": 6, "DIM_ITEM": 39, "DIM_LOCN": 35,
                          "FCT_SALES": 322, "FCT_INVENTORY": 115},
                         {name: len(rows) for name, rows in self.snapshot().items()})
        for filename, expected in self.expected.items():
            source = filename.rsplit("_", 1)[0]
            accepted = self.conn.execute(f"SELECT COUNT(*) FROM RETAIL_DWH.TMP.{source.upper()} WHERE SOURCE_FILE_NAME=?", [filename]).fetchone()[0]
            self.assertEqual(expected["accepted"], accepted, filename)
            if source == "pos_sales":
                amount = self.conn.execute("SELECT SUM(NET_SALES) FROM RETAIL_DWH.TMP.POS_SALES WHERE SOURCE_FILE_NAME=?", [filename]).fetchone()[0]
                self.assertEqual(expected["accepted_amount"], str(amount))
        # Replaying earlier subsets must not regress the latest descriptions,
        # surrogate keys, fact intervals, processing or rejection timestamps.
        baseline = self.snapshot()
        silver = {table: self.conn.execute(f"SELECT * FROM {table} ORDER BY SOURCE_ROW_ID").fetchall()
                  for table in [self.cfg["quarantine_table"]] + [n["tmp"] for n in self.cfg["sources"].values()]}
        for start, end in ((14, 16), (15, 18), (9, 18)):
            for number in range(start, end + 1):
                day = f"2026-11-{number:02d}"
                for source in SOURCES:
                    self.classify(source, day)
                    if source != "store_master" or number in (9, 11, 15):
                        verify_replay_decisions(self.cursor, self.cfg, source)
                self.run_gold(day)
                self.assertEqual(baseline, self.snapshot())
            self.assert_reconciled()
        for table, before in silver.items():
            self.assertEqual(before, self.conn.execute(f"SELECT * FROM {table} ORDER BY SOURCE_ROW_ID").fetchall())
        self.assertEqual("Household Care", self.conn.execute("SELECT SUBCLASS_NAME FROM RETAIL_DWH.TGT.DIM_SUBCLASS WHERE SUBCLASS_ID='SC03'").fetchone()[0])
        self.assertEqual(4, self.conn.execute("SELECT COUNT(*) FROM RETAIL_DWH.TGT.DIM_ITEM WHERE ITEM_ID='ITEM005'").fetchone()[0])
        self.assertEqual(3, self.conn.execute("SELECT COUNT(*) FROM RETAIL_DWH.TGT.DIM_LOCN WHERE STORE_ID='STORE007'").fetchone()[0])
        self.assertEqual(1, self.conn.execute("SELECT COUNT(*) FROM RETAIL_DWH.TGT.FCT_INVENTORY WHERE STORE_ID='STORE001' AND ITEM_ID='ITEM001' AND EFFECTIVE_FROM='2026-11-13'").fetchone()[0])

    def test_invalid_net_sales_is_quarantined_but_reconciliation_deliberately_fails(self):
        self.process_range(end=14)
        self.conn.execute("""UPDATE RETAIL_DWH.STG.POS_SALES SET
            RAW_PAYLOAD=json_merge_patch(RAW_PAYLOAD, '{"net_sales":"not-money"}')
            WHERE SOURCE_FILE_NAME='pos_sales_20261114.csv' AND SOURCE_ROW_NUMBER=1""")
        # Disposable database only: changing loaded inputs is NOT a supported
        # production correction workflow.
        self.classify_without_oracle("pos_sales", "2026-11-14")
        self.assertEqual("INVALID_NET_SALES", self.conn.execute("SELECT REJECT_REASON FROM RETAIL_DWH.TMP.WORK_POS_SALES WHERE SOURCE_ROW_NUMBER=1").fetchone()[0])
        rows = self.conn.execute(local_sql((ROOT / "include/sql/reconcile/reconcile_gold.sql").read_text())).fetchall()
        self.assertEqual("FAIL", rows[-1][-1])

    def classify_without_oracle(self, source, day):
        expected, self.expected = self.expected, {}
        try:
            self.classify(source, day)
        finally:
            self.expected = expected

    def test_reconciliation_and_integrity_detect_gold_corruption(self):
        self.process_range(end=14)
        for mutation in (
            "DELETE FROM RETAIL_DWH.TGT.FCT_SALES WHERE TRANSACTION_ID='EDGE14-001'",
            "UPDATE RETAIL_DWH.TGT.FCT_SALES SET NET_SALES=999 WHERE TRANSACTION_ID='EDGE14-001'",
            "UPDATE RETAIL_DWH.TGT.FCT_SALES SET ITEM_SK=-1 WHERE TRANSACTION_ID='EDGE14-001'",
            "DELETE FROM RETAIL_DWH.TGT.FCT_INVENTORY WHERE STORE_ID='STORE003' AND ITEM_ID='ITEM001'",
        ):
            with self.subTest(mutation=mutation):
                self.conn.execute("BEGIN")
                try:
                    self.conn.execute(mutation)
                    with self.assertRaises(AssertionError):
                        self.assert_reconciled()
                finally:
                    self.conn.execute("ROLLBACK")
        self.assert_reconciled()


class FixtureStructureTests(unittest.TestCase):
    def test_manifest_minimum_rows_headers_and_every_rejection_branch(self):
        ledger = json.loads((ROOT / "tests/fixtures/backfill_extension_expected.json").read_text())
        manifest = json.loads((ROOT / "data/file_manifest.json").read_text())
        self.assertEqual(16, len(ledger))
        self.assertEqual(len(manifest["files"]), manifest["file_count"])
        self.assertEqual(["2026-11-09", "2026-11-11", "2026-11-15"],
                         [d for d in manifest["store_snapshot_dates"] if "2026-11-09" <= d <= "2026-11-18"])
        for entry in manifest["files"]:
            path = ROOT / "data" / entry["file"]
            self.assertEqual(entry["sha256"], hashlib.sha256(path.read_bytes()).hexdigest())
            with path.open(newline="") as stream:
                rows = list(csv.reader(stream, strict=True))
            self.assertGreaterEqual(len(rows) - 1, 30)
            self.assertEqual(entry["rows"], len(rows) - 1)
            self.assertEqual(len(rows[0]), len(set(rows[0])))
            self.assertTrue(all(len(row) == len(rows[0]) for row in rows))
        for source, definition in SOURCES.items():
            reasons = set(re.findall(r"THEN '([A-Z_]+)'", (ROOT / "include/sql/silver" / definition["sql"]).read_text()))
            covered = {c["reason"] for filename, entry in ledger.items() if filename.startswith(source + "_") for c in entry["cases"]}
            # This branch necessarily makes production reconciliation FAIL.
            isolated = {"INVALID_NET_SALES"} if source == "pos_sales" else set()
            # New reporting-attribute failures have dedicated tests in
            # test_sales_mart.py rather than changing the original row oracle.
            if source == "item_master":
                isolated.add("INCOMPLETE_ITEM_HIERARCHY")
            if source == "store_master":
                isolated.add("NULL_LOCATION_TYPE")
            self.assertFalse(reasons - covered - isolated, (source, reasons - covered - isolated))

    def test_real_ingestion_rejects_structural_errors_and_preserves_raw_values(self):
        # Extract the actual dependency-free function rather than importing
        # Airflow or rewriting the CSV parser for local tests.
        tree = ast.parse((ROOT / "dags/full_etl_pipeline/stg_ingestion.py").read_text())
        function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "make_jsonl")
        namespace = {"csv": csv, "hashlib": hashlib, "json": json}
        exec(compile(ast.Module(body=[function], type_ignores=[]), "stg_ingestion.py", "exec"), namespace)
        with tempfile.TemporaryDirectory() as directory:
            path, output = Path(directory) / "fixture.csv", Path(directory) / "fixture.jsonl"
            for text in ('a,a\n1,2\n', 'a,b\n1\n', 'a,b\n1,2,3\n', 'a,\n1,2\n', 'a,b\n"unclosed,2\n', ''):
                path.write_text(text)
                with self.assertRaises((ValueError, csv.Error)):
                    namespace["make_jsonl"](path, output, "test")
            for name in json.loads((ROOT / "tests/fixtures/backfill_extension_expected.json").read_text()):
                fixture = ROOT / "data" / name
                count = namespace["make_jsonl"](fixture, output, "test")
                with fixture.open(newline="") as stream:
                    original = list(csv.DictReader(stream))
                envelopes = [json.loads(line) for line in output.read_text().splitlines()]
                self.assertEqual(len(original), count)
                self.assertEqual(original, [row["raw_payload"] for row in envelopes])
                self.assertEqual(count, len({row["source_row_id"] for row in envelopes}))
