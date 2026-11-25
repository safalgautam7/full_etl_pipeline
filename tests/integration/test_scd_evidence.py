"""Exercise SCD evidence failure detection locally; not Snowflake certification.

Only local test SQL substitutes JSON_OBJECT for OBJECT_CONSTRUCT_KEEP_NULL,
ordinary tables for session-local tables, and an IFF macro. No warehouse access.
"""

import csv
import unittest
from pathlib import Path

try:
    import duckdb
except ImportError:
    duckdb = None

ROOT = Path(__file__).resolve().parents[2]


@unittest.skipIf(duckdb is None, "Install requirements-test.txt for local evidence tests")
class ScdEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.conn = duckdb.connect(":memory:")
        self.addCleanup(self.conn.close)
        self.conn.execute("ATTACH ':memory:' AS RETAIL_DWH")
        for schema in ("TMP", "TGT"):
            self.conn.execute(f"CREATE SCHEMA RETAIL_DWH.{schema}")
        self.conn.execute("CREATE MACRO IFF(c, a, b) AS CASE WHEN c THEN a ELSE b END")
        self.conn.execute("""CREATE TABLE RETAIL_DWH.TMP.ITEM_MASTER (
            SNAPSHOT_DATE DATE, ITEM_ID VARCHAR, ITEM_NAME VARCHAR,
            SUBCLASS_ID VARCHAR, SUBCLASS_NAME VARCHAR, BRAND VARCHAR,
            UNIT_OF_MEASURE VARCHAR, LIST_PRICE DECIMAL(18,2), IS_ACTIVE BOOLEAN,
            DEPARTMENT_ID VARCHAR, DEPARTMENT_NAME VARCHAR, CLASS_ID VARCHAR, CLASS_NAME VARCHAR,
            SOURCE_FILE_NAME VARCHAR, SOURCE_ROW_NUMBER INTEGER, SOURCE_ROW_ID VARCHAR)""")
        self.conn.execute("""CREATE TABLE RETAIL_DWH.TMP.STORE_MASTER (
            SNAPSHOT_DATE DATE, STORE_ID VARCHAR, STORE_NAME VARCHAR,
            DISTRICT_ID VARCHAR, DISTRICT_NAME VARCHAR, CITY VARCHAR, REGION VARCHAR,
            IS_ACTIVE BOOLEAN, SOURCE_FILE_NAME VARCHAR, SOURCE_ROW_NUMBER INTEGER,
            SOURCE_ROW_ID VARCHAR, LOCATION_TYPE VARCHAR)""")
        fields = {
            "item_master": "snapshot_date item_id item_name subclass_id subclass_name brand unit_of_measure list_price is_active department_id department_name class_id class_name".split(),
            "store_master": "snapshot_date store_id store_name district_id district_name city region is_active".split(),
        }
        for source, columns in fields.items():
            for path in sorted((ROOT / "data").glob(f"{source}_*.csv")):
                if not "20261109" <= path.stem[-8:] <= "20261113":
                    continue  # This regression's oracle covers the original fixture.
                with path.open(newline="", encoding="utf-8-sig") as stream:
                    for number, row in enumerate(csv.DictReader(stream), 1):
                        values = [row[key] if key != "is_active" else row[key] == "Y" for key in columns]
                        values += [path.name, number, f"{path.name}:{number}"]
                        if source == "store_master":
                            values += [row["location_type"]]
                        self.conn.execute(
                            f"INSERT INTO RETAIL_DWH.TMP.{source.upper()} VALUES ({','.join('?' for _ in values)})",
                            values,
                        )
        for dimension, source, natural, name, sk in (
            ("DIM_SUBCLASS", "ITEM_MASTER", "SUBCLASS_ID", "SUBCLASS_NAME", "SUBCLASS_SK"),
            ("DIM_DISTRICT", "STORE_MASTER", "DISTRICT_ID", "DISTRICT_NAME", "DISTRICT_SK"),
        ):
            self.conn.execute(f"""CREATE TABLE RETAIL_DWH.TGT.{dimension} AS
                SELECT {natural}, {name}, ROW_NUMBER() OVER (ORDER BY {natural}) AS {sk}
                FROM (SELECT {natural}, {name} FROM RETAIL_DWH.TMP.{source}
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY {natural}
                        ORDER BY SNAPSHOT_DATE DESC, SOURCE_FILE_NAME DESC,
                                 SOURCE_ROW_NUMBER DESC, SOURCE_ROW_ID DESC) = 1)""")
        for suffix, sk in (("item", "ITEM_SK"), ("locn", "LOCN_SK")):
            sql = (ROOT / f"include/sql/gold/prepare_dim_{suffix}.sql").read_text()
            self.conn.execute(sql.replace("TEMPORARY TABLE", "TABLE"))
            self.conn.execute(f"""CREATE TABLE RETAIL_DWH.TGT.DIM_{suffix.upper()} AS
                SELECT ROW_NUMBER() OVER () AS {sk}, *
                FROM RETAIL_DWH.TMP.WORK_DIM_{suffix.upper()}""")

    def audit(self, section):
        sql = (ROOT / "evidence/verify_etl_stages.sql").read_text()
        sql = sql.split(f"-- {section}.", 1)[1].split(f"-- F{int(section[1:]) + 1}.", 1)[0]
        sql = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
        # Skip the section-title continuation left by splitting at '-- Fx.'.
        sql = sql[sql.index("WITH "):]
        cursor = self.conn.execute(sql.replace("OBJECT_CONSTRUCT_KEEP_NULL", "JSON_OBJECT"))
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def assert_clean(self, section):
        rows = self.audit(section)
        self.assertEqual(len([r for r in rows if r["ROW_KIND"] == "SUMMARY"]), 2)
        self.assertTrue(all(r["STATUS"] == "PASS" for r in rows), rows)

    def assert_failure(self, section):
        self.assertTrue(any(r["ROW_KIND"] == "SUMMARY" and r["STATUS"] == "FAIL"
                            for r in self.audit(section)))

    def test_valid_fixture_passes_both_audits(self):
        self.assert_clean("F2")
        self.assert_clean("F3")

    def test_scd1_wrong_description_duplicate_and_extra_key_fail(self):
        self.conn.execute("UPDATE RETAIL_DWH.TGT.DIM_SUBCLASS SET SUBCLASS_NAME='wrong' WHERE SUBCLASS_ID='SC03'")
        self.assert_failure("F2")
        self.conn.execute("INSERT INTO RETAIL_DWH.TGT.DIM_DISTRICT SELECT * FROM RETAIL_DWH.TGT.DIM_DISTRICT LIMIT 1")
        self.assert_failure("F2")
        self.conn.execute("INSERT INTO RETAIL_DWH.TGT.DIM_SUBCLASS VALUES ('EXTRA', 'Extra', 999)")
        self.assert_failure("F2")

    def test_missing_version_detected(self):
        self.conn.execute("DELETE FROM RETAIL_DWH.TGT.DIM_ITEM WHERE ITEM_ID='ITEM005' AND NOT IS_CURRENT")
        self.assert_failure("F3")

    def test_each_scd2_corruption_detected(self):
        mutations = [
            "LIST_PRICE=99.00", "EFFECTIVE_TO=DATE '2026-11-10'",
            "IS_CURRENT=TRUE", "SUBCLASS_SK=999", "RECORD_HASH=0",
            "SOURCE_ROW_ID='wrong'", "ITEM_SK=NULL",
        ]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                self.conn.execute("BEGIN")
                try:
                    self.conn.execute(f"UPDATE RETAIL_DWH.TGT.DIM_ITEM SET {mutation} WHERE ITEM_ID='ITEM005' AND NOT IS_CURRENT")
                    self.assert_failure("F3")
                finally:
                    self.conn.execute("ROLLBACK")

    def test_duplicate_version_and_surrogate_fail(self):
        self.conn.execute("INSERT INTO RETAIL_DWH.TGT.DIM_LOCN SELECT * FROM RETAIL_DWH.TGT.DIM_LOCN LIMIT 1")
        self.assert_failure("F3")

    def test_empty_sources_and_targets_fail_summaries(self):
        for table in ("TMP.ITEM_MASTER", "TMP.STORE_MASTER", "TGT.DIM_SUBCLASS",
                      "TGT.DIM_DISTRICT", "TGT.DIM_ITEM", "TGT.DIM_LOCN"):
            self.conn.execute(f"DELETE FROM RETAIL_DWH.{table}")
        self.assert_failure("F2")
        self.assert_failure("F3")


if __name__ == "__main__":
    unittest.main()
