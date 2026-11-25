"""Opt-in real Snowflake acceptance, restricted to disposable test schemas.

Uses the same external credentials/write guards as test_snowflake_silver.py.
Never creates, changes or drops anything in RETAIL_DWH.
"""

import csv
import hashlib
import json
import unittest
import uuid
from datetime import date
from pathlib import Path

from full_etl_pipeline.corrections import RepairSQL, assert_checks, correct_file, repair_gold, repair_silver
from full_etl_pipeline.reporting import load_calendar
from full_etl_pipeline.silver_processing import SOURCES
from test_snowflake_silver import ENABLED, guarded_settings

ROOT = Path(__file__).resolve().parents[2]


@unittest.skipUnless(ENABLED, "Snowflake writes disabled; use isolated opt-in test database")
class SnowflakeReportingCorrectionTests(unittest.TestCase):
    def test_calendar_correction_replay_history_and_atomic_rollback(self):
        import snowflake.connector

        settings = guarded_settings()  # Validate BEFORE connecting.
        database = settings["database"]
        suffix = uuid.uuid4().hex[:12].upper()
        schemas = {s: f"{s}_TEST_{suffix}" for s in ("STG", "TMP", "DQ", "TGT", "CONTROL")}
        conn = snowflake.connector.connect(**settings)
        self.addCleanup(conn.close)
        conn.autocommit(True)
        raw = conn.cursor()
        self.addCleanup(raw.close)

        def drop_owned_schema(schema):
            with conn.cursor() as cleanup:
                cleanup.execute(f"DROP SCHEMA {database}.{schema}")

        for schema in schemas.values():
            raw.execute(f"CREATE SCHEMA {database}.{schema}")
            self.addCleanup(drop_owned_schema, schema)

        class Cursor:
            fail = False

            def execute(self, sql, params=None):
                if self.fail and "MERGE INTO RETAIL_DWH.TGT.FCT_INVENTORY_DAILY" in sql:
                    raise RuntimeError("injected acceptance failure")
                for original, actual in schemas.items():
                    sql = sql.replace(f"RETAIL_DWH.{original}.", f"{database}.{actual}.")
                raw.execute(sql, params)
                return self

            def fetchone(self):
                return raw.fetchone()

            def fetchall(self):
                return raw.fetchall()

        cur = Cursor()
        for filename in ("create_tmp_dq_tables.sql", "create_tgt_dimensions.sql", "create_tgt_facts.sql",
                         "create_reporting_correction_tables.sql"):
            ddl = (ROOT / "include/sql/ddl" / filename).read_text()
            ddl = "\n".join(line for line in ddl.splitlines() if not line.lstrip().startswith("--"))
            for statement in ddl.split(";"):
                if statement.strip().startswith("CREATE TABLE"):
                    cur.execute(statement)
        manifest_ddl = (ROOT / "include/sql/control/01_file_manifest.sql").read_text()
        manifest_ddl = "\n".join(line for line in manifest_ddl.splitlines() if not line.lstrip().startswith("--"))
        for statement in manifest_ddl.split(";"):
            if statement.strip().startswith("CREATE TABLE"):
                cur.execute(statement)

        cfg = json.loads((ROOT / "config/retail_tmp.json").read_text())
        gold = {"gold_sql_dir": str(ROOT / "include/sql/gold")}
        load_calendar(cur, gold)
        cur.execute("SELECT COUNT(*) FROM RETAIL_DWH.TGT.DIM_DATE")
        self.assertEqual((date(2036, 1, 1) - date(2020, 1, 1)).days, cur.fetchone()[0])
        cur.execute("SELECT ISO_WEEK_YEAR,ISO_WEEK_NUMBER FROM RETAIL_DWH.TGT.DIM_DATE WHERE CALENDAR_DATE='2027-01-01'")
        self.assertEqual((2026, 53), cur.fetchone())
        cur.execute("SELECT COUNT(*) FROM RETAIL_DWH.TGT.DIM_DATE WHERE CALENDAR_DATE='2024-02-29'")
        self.assertEqual(1, cur.fetchone()[0])
        load_calendar(cur, gold)

        for source in SOURCES:
            cur.execute(f"CREATE TABLE RETAIL_DWH.STG.{source.upper()} ("
                        "SOURCE_ROW_ID VARCHAR(64),SOURCE_FILE_ID VARCHAR(64),SOURCE_FILE_NAME VARCHAR(255),"
                        "SOURCE_FILE_DATE DATE,SOURCE_ROW_NUMBER NUMBER(38,0),BATCH_ID VARCHAR(100),RAW_PAYLOAD VARIANT)")

        def insert_rows(table, rows, filename, digest, batch):
            file_id = hashlib.sha256(f"{filename}:{digest}".encode()).hexdigest()
            day = f"{filename[-12:-8]}-{filename[-8:-6]}-{filename[-6:-4]}"
            for number, row in enumerate(rows, 1):
                row_id = hashlib.sha256(f"{file_id}:{number}".encode()).hexdigest()
                cur.execute(f"INSERT INTO {table} SELECT %s,%s,%s,TO_DATE(%s),%s,%s,PARSE_JSON(%s)",
                            (row_id, file_id, filename, day, number, batch, json.dumps(row)))
            return file_id, day

        originals = {}
        for path in sorted((ROOT / "data").glob("*.csv")):
            if not "20261109" <= path.stem[-8:] <= "20261113":
                continue
            source = path.name.rsplit("_", 1)[0]
            with path.open(newline="", encoding="utf-8-sig") as stream:
                rows = list(csv.DictReader(stream))
            originals[path.name] = rows
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            file_id, day = insert_rows(f"RETAIL_DWH.STG.{source.upper()}", rows, path.name, digest, "fixture")
            cur.execute("INSERT INTO RETAIL_DWH.CONTROL.FILE_MANIFEST "
                        "(SOURCE_FILE_ID,SOURCE_FILE_NAME,SOURCE_FILE_SHA256,SOURCE_FILE_DATE,SOURCE_TYPE,"
                        "STG_TABLE,ROW_COUNT,LOAD_STATUS,BATCH_ID) VALUES (%s,%s,%s,TO_DATE(%s),%s,%s,%s,'LOADED','fixture')",
                        (file_id, path.name, digest, day, source, f"RETAIL_DWH.STG.{source.upper()}", len(rows)))

        runner = RepairSQL(cur, ROOT / "include/sql")
        for source in SOURCES.values():
            runner.prepare("silver", source["sql"])
        for name in ("prepare_dim_item.sql", "prepare_dim_locn.sql", "prepare_fct_sales.sql",
                     "resolve_inventory_observations.sql", "prepare_fct_inventory.sql"):
            runner.prepare("gold", name)
        cur.execute("BEGIN")
        try:
            repair_silver(cur, cfg, runner)
            repair_gold(cur, runner)
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise

        def stage(filename, rows, identifier):
            source = filename.rsplit("_", 1)[0]
            cur.execute("SELECT SOURCE_FILE_SHA256 FROM RETAIL_DWH.CONTROL.FILE_MANIFEST WHERE SOURCE_FILE_NAME=%s", (filename,))
            old_sha = cur.fetchone()[0]
            digest = hashlib.sha256(json.dumps(rows).encode()).hexdigest()
            cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE RETAIL_DWH.STG.REPLACEMENT_INPUT LIKE RETAIL_DWH.STG.{source.upper()}")
            file_id, _ = insert_rows("RETAIL_DWH.STG.REPLACEMENT_INPUT", rows, filename, digest, identifier)
            return dict(filename=filename, source=source, old_sha=old_sha, new_sha=digest,
                        new_file_id=file_id, correction_id=identifier, reason="Acceptance test", approved_by="test operator")

        request = stage("pos_sales_20261109.csv", originals["pos_sales_20261109.csv"][:-1], "sales-correction")
        self.assertEqual("SUCCEEDED", correct_file(cur, cfg, ROOT / "include/sql", **request))
        request["correction_id"] = "sales-replay"
        self.assertEqual("NOOP", correct_file(cur, cfg, ROOT / "include/sql", **request))
        request = stage("item_master_20261109.csv", [r for r in originals["item_master_20261109.csv"]
                                                   if r["item_id"] != "ITEM001"], "master-correction")
        self.assertEqual("SUCCEEDED", correct_file(cur, cfg, ROOT / "include/sql", **request))
        request = stage("inventory_snapshot_20261110.csv", originals["inventory_snapshot_20261110.csv"][:-1], "inventory-correction")
        self.assertEqual("SUCCEEDED", correct_file(cur, cfg, ROOT / "include/sql", **request))

        tables = [f"RETAIL_DWH.{layer}.{source.upper()}" for layer in ("STG", "TMP") for source in SOURCES]
        tables += [f"RETAIL_DWH.TGT.{name}" for name in ("DIM_ITEM", "DIM_LOCN", "FCT_SALES", "FCT_INVENTORY", "FCT_INVENTORY_DAILY")]
        tables += ["RETAIL_DWH.CONTROL.FILE_MANIFEST", "RETAIL_DWH.DQ.QUARANTINE_RECORDS"]

        def snapshot():
            result = {}
            for table in tables:
                cur.execute(f"SELECT * FROM {table} ORDER BY 1")
                result[table] = cur.fetchall()
            return result

        request = stage("pos_sales_20261110.csv", [], "rollback-correction")
        before = snapshot()
        cur.fail = True
        with self.assertRaisesRegex(RuntimeError, "injected acceptance failure"):
            correct_file(cur, cfg, ROOT / "include/sql", **request)
        cur.fail = False
        self.assertEqual(before, snapshot())
        for name in ("check_gold_integrity.sql", "check_reporting_integrity.sql"):
            runner.run("reconcile", name)
            assert_checks(cur, name)
