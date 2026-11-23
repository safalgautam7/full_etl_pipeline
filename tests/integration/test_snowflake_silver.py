"""Opt-in real Snowflake fixture/replay test. Never writes to RETAIL_DWH.

Requires an EXISTING database named RETAIL_ETL_TEST_*, a local connector config
file outside this repository, and RETAIL_TEST_ALLOW_WRITES=1. Creates/drops only
three uniquely named schemas owned by this test invocation. Ordinary local test
commands skip it. No connections or schemas are created unless explicitly enabled.
"""

import csv
import hashlib
import json
import os
import re
import tempfile
import unittest
import uuid
from pathlib import Path

from full_etl_pipeline.silver_processing import SOURCES, process_source

ROOT = Path(__file__).resolve().parents[2]
ENABLED = os.environ.get("RETAIL_TEST_ALLOW_WRITES") == "1"


def guarded_settings():
    path = Path(os.environ["RETAIL_TEST_SNOWFLAKE_CONFIG"]).expanduser().resolve()
    if path.is_relative_to(ROOT):
        raise ValueError("Keep Snowflake connector credentials outside the repository")
    cfg = json.loads(path.read_text(encoding="utf-8"))
    database = cfg.get("database", "")
    if not re.fullmatch(r"RETAIL_ETL_TEST_[A-Z0-9_]+", database):
        raise ValueError("Tests require an existing RETAIL_ETL_TEST_* database; never RETAIL_DWH")
    return cfg


@unittest.skipUnless(ENABLED, "Snowflake writes are disabled; see README for isolated opt-in verification")
class SnowflakeSilverTests(unittest.TestCase):
    def test_fixture_replay_preserves_business_data_lineage_and_timestamps(self):
        import snowflake.connector

        settings = guarded_settings()  # Guard BEFORE opening a connection.
        database = settings["database"]
        suffix = uuid.uuid4().hex[:12].upper()
        schema_map = {name: f"{name}_TEST_{suffix}" for name in ("STG", "TMP", "DQ")}
        conn = snowflake.connector.connect(**settings)
        self.addCleanup(conn.close)
        conn.autocommit(True)
        cursor = conn.cursor()
        self.addCleanup(cursor.close)

        def drop_owned_schema(name):
            with conn.cursor() as cleanup:
                cleanup.execute(f"DROP SCHEMA {database}.{name}")

        for schema in schema_map.values():
            cursor.execute(f"CREATE SCHEMA {database}.{schema}")
            self.addCleanup(drop_owned_schema, schema)

        def rewrite(sql):
            for original, actual in schema_map.items():
                sql = sql.replace(f"RETAIL_DWH.{original}.", f"{database}.{actual}.")
            return sql

        ddl = (ROOT / "include/sql/ddl/create_tmp_dq_tables.sql").read_text()
        ddl = "\n".join(line for line in ddl.splitlines() if not line.lstrip().startswith("--"))
        for statement in ddl.split(";"):
            if statement.strip().startswith("CREATE TABLE"):
                cursor.execute(rewrite(statement))

        cfg = {
            "snowflake_conn_id": "retail_snowflake",
            "quarantine_table": f"{database}.{schema_map['DQ']}.QUARANTINE_RECORDS",
            "sources": {},
        }
        for source in SOURCES:
            table = source.upper()
            cfg["sources"][source] = {
                "stg": f"{database}.{schema_map['STG']}.{table}",
                "tmp": f"{database}.{schema_map['TMP']}.{table}",
                "work": f"{database}.{schema_map['TMP']}.WORK_{table}",
            }
            cursor.execute(f"""CREATE TABLE {cfg['sources'][source]['stg']} (
                SOURCE_ROW_ID VARCHAR(64), SOURCE_FILE_ID VARCHAR(64),
                SOURCE_FILE_NAME VARCHAR(255), SOURCE_FILE_DATE DATE,
                SOURCE_ROW_NUMBER NUMBER(38,0), BATCH_ID VARCHAR(100), RAW_PAYLOAD VARIANT
            )""")
            for path in sorted((ROOT / "data").glob(f"{source}_*.csv")):
                if path.stem[-8:] > "20261113":
                    continue  # Original 510-row acceptance oracle below.
                file_hash = hashlib.sha256(path.read_bytes()).hexdigest()
                file_id = hashlib.sha256(f"{path.name}:{file_hash}".encode()).hexdigest()
                file_date = path.stem.rsplit("_", 1)[1]
                with path.open(newline="", encoding="utf-8-sig") as stream:
                    for number, row in enumerate(csv.DictReader(stream), 1):
                        row_id = hashlib.sha256(f"{file_id}:{number}".encode()).hexdigest()
                        cursor.execute(
                            f"INSERT INTO {cfg['sources'][source]['stg']} "
                            "SELECT %s,%s,%s,TO_DATE(%s,'YYYYMMDD'),%s,%s,PARSE_JSON(%s)",
                            (row_id, file_id, path.name, file_date, number, "isolated-test", json.dumps(row)),
                        )

        def snapshot():
            result = {}
            tables = [cfg["quarantine_table"]]
            tables += [names[key] for names in cfg["sources"].values() for key in ("stg", "tmp")]
            for table in tables:
                cursor.execute(f"SELECT * FROM {table} ORDER BY SOURCE_ROW_ID")
                result[table] = cursor.fetchall()
            return result

        # Rewrite ONLY test copies, not the production SQL or working database.
        with tempfile.TemporaryDirectory(prefix="retail_silver_test_") as directory:
            for path in (ROOT / "include/sql/silver").glob("*.sql"):
                (Path(directory) / path.name).write_text(rewrite(path.read_text()), encoding="utf-8")
            results = [process_source(cursor, cfg, source, directory) for source in SOURCES]
            self.assertEqual(510, sum(row["raw"] for row in results))
            self.assertEqual(505, sum(row["accepted"] for row in results))
            self.assertEqual(5, sum(row["quarantined"] for row in results))
            cursor.execute(f"SELECT REASON_CODE,COUNT(*) FROM {cfg['quarantine_table']} GROUP BY REASON_CODE")
            self.assertEqual({"NULL_ITEM_ID": 2, "NEGATIVE_QUANTITY": 1, "UNKNOWN_ITEM_ID": 1, "DUPLICATE_SALES_LINE": 1}, dict(cursor.fetchall()))
            baseline = snapshot()
            for source in SOURCES:
                process_source(cursor, cfg, source, directory)
            self.assertEqual(baseline, snapshot(), "Replay changed rows, lineage, or processing/rejection timestamps")


if __name__ == "__main__":
    unittest.main()
