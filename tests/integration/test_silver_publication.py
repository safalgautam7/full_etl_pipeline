"""Execute the actual publication SQL against a disposable in-memory database.

DuckDB is NOT proof of Snowflake-specific CSV/VARIANT/casting behavior. These
tests cover conditional MERGE, exact values, dispositions, and rollback without
ever connecting to the working warehouse. The existing classifier SQL is not
translated or replaced in production.
"""

import unittest
from full_etl_pipeline.daily_processing import verify_replay_decisions
from pathlib import Path

from full_etl_pipeline.silver_processing import (
    LINEAGE_COLS,
    SOURCES,
    load_config,
    publish_work,
)

try:
    import duckdb
except ImportError:
    duckdb = None

ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = ROOT / "include/sql/silver"


class Cursor:
    def __init__(self, conn):
        self.conn = conn
        self.statements = []
        self.fail_on = None

    def execute(self, sql, params=None):
        self.statements.append(sql)
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("injected publication failure")
        self.conn.execute(sql.replace("%s", "?"), params or [])
        return self

    def fetchone(self):
        return self.conn.fetchone()


@unittest.skipIf(duckdb is None, "Install requirements-test.txt to run local SQL regression tests")
class SilverPublicationTests(unittest.TestCase):
    def setUp(self):
        self.conn = duckdb.connect(":memory:")
        self.addCleanup(self.conn.close)
        self.conn.execute("ATTACH ':memory:' AS RETAIL_DWH")
        for schema in ("STG", "TMP", "DQ"):
            self.conn.execute(f"CREATE SCHEMA RETAIL_DWH.{schema}")
        self.cfg = load_config(ROOT / "config/retail_tmp.json")
        # Use the checked-in schema, changing only Snowflake-specific type names.
        ddl = (ROOT / "include/sql/ddl/create_tmp_dq_tables.sql").read_text()
        ddl = "\n".join(line for line in ddl.splitlines() if not line.lstrip().startswith("--"))
        for statement in ddl.split(";"):
            if statement.strip().startswith("CREATE TABLE"):
                self.conn.execute(
                    statement.replace("TIMESTAMP_LTZ", "TIMESTAMPTZ")
                    .replace("VARIANT", "JSON").replace("NUMBER(", "DECIMAL(")
                    .replace("CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP")
                )
        for source, definition in SOURCES.items():
            names = self.cfg["sources"][source]
            self.conn.execute(f"CREATE TABLE {names['stg']} (SOURCE_ROW_ID VARCHAR, RAW_PAYLOAD JSON)")
            self.conn.execute(
                f"CREATE TABLE {names['work']} AS SELECT {LINEAGE_COLS}, {definition['cols']}, "
                f"CAST(NULL AS VARCHAR) REJECT_REASON, CAST(NULL AS JSON) RAW_PAYLOAD "
                f"FROM {names['tmp']} WHERE 1=0"
            )
        self.cursor = Cursor(self.conn)

    def append(self, source, row_id, reason=None):
        names = self.cfg["sources"][source]
        self.conn.execute(f"INSERT INTO {names['stg']} VALUES (?, ?)", [row_id, '{"immutable":true}'])
        values = {
            "SOURCE_ROW_ID": row_id, "SOURCE_FILE_ID": "file-id", "SOURCE_FILE_NAME": source + "_20261109.csv",
            "SOURCE_FILE_DATE": "2026-11-09", "SOURCE_ROW_NUMBER": 1, "BATCH_ID": "original-ingestion",
            "SNAPSHOT_DATE": "2026-11-09", "SALE_DATE": "2026-11-09",
            "ITEM_ID": "ITEM001", "ITEM_NAME": "Example", "SUBCLASS_ID": "SC01", "SUBCLASS_NAME": "Subclass",
            "BRAND": None, "UNIT_OF_MEASURE": "EA", "LIST_PRICE": "10.00", "IS_ACTIVE": True,
            "STORE_ID": "STORE001", "STORE_NAME": "Store", "DISTRICT_ID": "DIST01", "DISTRICT_NAME": "District",
            "CITY": None, "REGION": "North", "TRANSACTION_ID": row_id, "LINE_NUMBER": 1,
            "QUANTITY": 2, "UNIT_PRICE": "10.00", "DISCOUNT_AMOUNT": "1.00", "NET_SALES": "19.00",
            "PAYMENT_METHOD": None, "QUANTITY_ON_HAND": 3, "UNIT_COST": "5.00", "INVENTORY_VALUE": "15.00",
            "REJECT_REASON": reason, "RAW_PAYLOAD": '{"immutable":true}',
        }
        columns = (LINEAGE_COLS + ", " + SOURCES[source]["cols"] + ", REJECT_REASON, RAW_PAYLOAD").split(", ")
        self.conn.execute(
            f"INSERT INTO {names['work']} ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})",
            [values[column] for column in columns],
        )

    def publish(self, source):
        return publish_work(self.cursor, self.cfg, source, SQL_DIR)

    def snapshot(self):
        tables = [self.cfg["quarantine_table"]]
        tables += [names[key] for names in self.cfg["sources"].values() for key in ("stg", "tmp")]
        return {table: self.conn.execute(f"SELECT * FROM {table} ORDER BY SOURCE_ROW_ID").fetchall() for table in tables}

    def age_timestamps(self, source):
        self.conn.execute(f"UPDATE {self.cfg['sources'][source]['tmp']} SET PROCESSED_AT='2000-01-01'")
        self.conn.execute(f"UPDATE {self.cfg['quarantine_table']} SET REJECTED_AT='2000-01-01'")

    def test_identical_replay_preserves_all_columns_and_timestamps_for_all_sources(self):
        for source in SOURCES:
            with self.subTest(source=source):
                self.append(source, source + "-accepted")
                self.append(source, source + "-rejected", "TEST_REJECTION")
                result = self.publish(source)
                self.assertEqual((2, 1, 1), (result["raw"], result["accepted"], result["quarantined"]))
                self.age_timestamps(source)
                before = self.snapshot()
                self.publish(source)
                self.assertEqual(before, self.snapshot())

    def test_historical_replay_accepts_identical_decisions_and_blocks_rule_changes(self):
        for source in SOURCES:
            with self.subTest(source=source):
                self.append(source, source + "-accepted")
                self.append(source, source + "-rejected", "TEST_REJECTION")
                self.publish(source)
                before = self.snapshot()
                verify_replay_decisions(self.cursor, self.cfg, source)
                work = self.cfg["sources"][source]["work"]
                self.conn.execute(f"UPDATE {work} SET REJECT_REASON='CHANGED_RULE' WHERE REJECT_REASON IS NOT NULL")
                with self.assertRaisesRegex(RuntimeError, "historical replay would change"):
                    verify_replay_decisions(self.cursor, self.cfg, source)
                self.assertEqual(before, self.snapshot())

    def test_historical_replay_blocks_new_decisions_and_reclassification(self):
        self.append("pos_sales", "accepted")
        self.publish("pos_sales")
        before = self.snapshot()
        work = self.cfg["sources"]["pos_sales"]["work"]
        self.conn.execute(f"UPDATE {work} SET REJECT_REASON='NEW_REJECTION'")
        with self.assertRaisesRegex(RuntimeError, "historical replay would change"):
            verify_replay_decisions(self.cursor, self.cfg, "pos_sales")
        self.assertEqual(before, self.snapshot())

    def test_new_row_is_inserted_once(self):
        self.append("pos_sales", "one")
        self.publish("pos_sales")
        self.append("pos_sales", "two")
        self.assertEqual(2, self.publish("pos_sales")["accepted"])
        before = self.snapshot()
        self.publish("pos_sales")
        self.assertEqual(before, self.snapshot())

    def test_changed_value_and_null_transitions_update_only_one_row(self):
        self.append("pos_sales", "changed")
        self.append("pos_sales", "untouched")
        self.publish("pos_sales")
        self.age_timestamps("pos_sales")
        original = self.snapshot()
        work = self.cfg["sources"]["pos_sales"]["work"]
        self.conn.execute(f"UPDATE {work} SET NET_SALES=18.00, DISCOUNT_AMOUNT=2.00, PAYMENT_METHOD='CASH' WHERE SOURCE_ROW_ID='changed'")
        self.publish("pos_sales")
        after = self.snapshot()
        target = self.cfg["sources"]["pos_sales"]["tmp"]
        self.assertNotEqual(original[target][0], after[target][0])
        self.assertEqual(original[target][1], after[target][1])
        self.assertEqual(original[self.cfg["sources"]["pos_sales"]["stg"]], after[self.cfg["sources"]["pos_sales"]["stg"]])
        self.conn.execute(f"UPDATE {work} SET PAYMENT_METHOD=NULL WHERE SOURCE_ROW_ID='changed'")
        self.publish("pos_sales")
        self.assertIsNone(self.conn.execute(f"SELECT PAYMENT_METHOD FROM {target} WHERE SOURCE_ROW_ID='changed'").fetchone()[0])

    def test_each_business_field_propagates_for_every_source(self):
        for source, definition in SOURCES.items():
            self.append(source, source)
            self.publish(source)
            names = self.cfg["sources"][source]
            for column in definition["cols"].split(", "):
                with self.subTest(source=source, column=column):
                    value = self.conn.execute(f"SELECT {column} FROM {names['work']}").fetchone()[0]
                    if value is None:
                        replacement = "new optional value"
                    elif isinstance(value, bool):
                        replacement = not value
                    elif isinstance(value, str):
                        replacement = value + " changed"
                    else:
                        from datetime import date, timedelta
                        replacement = value + timedelta(days=1) if isinstance(value, date) else value + 1
                    self.conn.execute(f"UPDATE {names['work']} SET {column}=?", [replacement])
                    self.publish(source)
                    self.assertEqual(
                        self.conn.execute(f"SELECT {column} FROM {names['work']}").fetchone(),
                        self.conn.execute(f"SELECT {column} FROM {names['tmp']}").fetchone(),
                    )

    def test_reclassification_both_directions_is_exclusive(self):
        self.append("pos_sales", "one")
        self.publish("pos_sales")
        work = self.cfg["sources"]["pos_sales"]["work"]
        self.conn.execute(f"UPDATE {work} SET REJECT_REASON='RULE_CHANGED'")
        self.assertEqual(1, self.publish("pos_sales")["quarantined"])
        self.conn.execute(f"UPDATE {work} SET REJECT_REASON=NULL")
        self.assertEqual(1, self.publish("pos_sales")["accepted"])
        self.assertEqual(0, self.conn.execute("SELECT COUNT(*) FROM RETAIL_DWH.DQ.QUARANTINE_RECORDS").fetchone()[0])

    def test_rejected_reason_and_payload_update_without_mutating_stg(self):
        self.append("pos_sales", "one", "FIRST_REASON")
        self.publish("pos_sales")
        self.age_timestamps("pos_sales")
        before = self.snapshot()
        self.conn.execute("UPDATE RETAIL_DWH.TMP.WORK_POS_SALES SET REJECT_REASON='SECOND_REASON'")
        self.publish("pos_sales")
        row = self.conn.execute("SELECT REASON_CODE, REJECTED_AT FROM RETAIL_DWH.DQ.QUARANTINE_RECORDS").fetchone()
        self.assertEqual("SECOND_REASON", row[0])
        self.assertNotEqual(2000, row[1].year)
        # Payload comparison is also conditional; immutable STG remains untouched.
        self.conn.execute("UPDATE RETAIL_DWH.TMP.WORK_POS_SALES SET RAW_PAYLOAD='{\"extra\":true}'")
        self.publish("pos_sales")
        self.assertEqual('{"extra":true}', self.conn.execute("SELECT RAW_PAYLOAD FROM RETAIL_DWH.DQ.QUARANTINE_RECORDS").fetchone()[0])
        self.assertEqual(before["RETAIL_DWH.STG.POS_SALES"], self.snapshot()["RETAIL_DWH.STG.POS_SALES"])

    def test_fault_between_transition_and_merge_rolls_back_everything(self):
        self.append("pos_sales", "accepted")
        self.append("pos_sales", "rejected", "BAD")
        self.publish("pos_sales")
        before = self.snapshot()
        self.conn.execute("UPDATE RETAIL_DWH.TMP.WORK_POS_SALES SET REJECT_REASON=CASE WHEN REJECT_REASON IS NULL THEN 'BAD' ELSE NULL END")
        self.cursor.fail_on = "MERGE INTO RETAIL_DWH.DQ.QUARANTINE_RECORDS"
        with self.assertRaisesRegex(RuntimeError, "injected"):
            self.publish("pos_sales")
        self.assertEqual(before, self.snapshot())
        self.assertEqual("ROLLBACK", self.cursor.statements[-1])

    def test_duplicate_work_identity_fails_before_begin(self):
        self.append("pos_sales", "one")
        self.conn.execute("INSERT INTO RETAIL_DWH.TMP.WORK_POS_SALES SELECT * FROM RETAIL_DWH.TMP.WORK_POS_SALES")
        before = self.snapshot()
        with self.assertRaisesRegex(RuntimeError, "row identity mismatch"):
            self.publish("pos_sales")
        self.assertNotIn("BEGIN", self.cursor.statements)
        self.assertEqual(before, self.snapshot())

    def test_equal_count_but_wrong_work_identity_fails(self):
        self.append("pos_sales", "one")
        self.conn.execute("UPDATE RETAIL_DWH.TMP.WORK_POS_SALES SET SOURCE_ROW_ID='foreign'")
        with self.assertRaisesRegex(RuntimeError, "foreign rows=1"):
            self.publish("pos_sales")
        self.assertNotIn("BEGIN", self.cursor.statements)

    def test_duplicate_target_fails_and_rolls_back(self):
        self.append("pos_sales", "one")
        self.publish("pos_sales")
        self.conn.execute("INSERT INTO RETAIL_DWH.TMP.POS_SALES SELECT * FROM RETAIL_DWH.TMP.POS_SALES")
        before = self.snapshot()
        with self.assertRaisesRegex(RuntimeError, "disposition mismatch"):
            self.publish("pos_sales")
        self.assertEqual(before, self.snapshot())

    def test_other_source_quarantine_and_unrelated_rows_are_untouched(self):
        self.append("item_master", "same-id", "ITEM_REASON")
        self.publish("item_master")
        self.append("pos_sales", "same-id", "SALES_REASON")
        self.publish("pos_sales")
        self.conn.execute("UPDATE RETAIL_DWH.TMP.WORK_POS_SALES SET REJECT_REASON=NULL")
        self.publish("pos_sales")
        self.assertEqual([("item_master", "ITEM_REASON")], self.conn.execute(
            "SELECT SOURCE_TYPE, REASON_CODE FROM RETAIL_DWH.DQ.QUARANTINE_RECORDS"
        ).fetchall())


if __name__ == "__main__":
    unittest.main()
