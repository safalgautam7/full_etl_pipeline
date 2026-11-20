"""Run actual Gold SQL sequentially for five days in local DuckDB.

Exercises incremental publication and unchanged replay, not Snowflake PUT/COPY
or API execution. Accepted fixture sales below are test inputs, not a substitute
for production classifier verification.
"""

import csv
import re
from pathlib import Path
import unittest

from full_etl_pipeline.daily_processing import scoped_gold_sql
import test_scd_evidence as evidence

ROOT = Path(__file__).resolve().parents[2]


@unittest.skipIf(evidence.duckdb is None, "Install requirements-test.txt")
class DailyGoldTests(unittest.TestCase):
    def setUp(self):
        evidence.ScdEvidenceTests.setUp(self)
        for table in ("ITEM_MASTER", "STORE_MASTER"):
            self.conn.execute(f"CREATE TABLE RETAIL_DWH.TMP.ALL_{table} AS SELECT * FROM RETAIL_DWH.TMP.{table}")
            self.conn.execute(f"DELETE FROM RETAIL_DWH.TMP.{table}")
        for table, sk in (("DIM_SUBCLASS", "SUBCLASS_SK"), ("DIM_DISTRICT", "DISTRICT_SK"),
                          ("DIM_ITEM", "ITEM_SK"), ("DIM_LOCN", "LOCN_SK")):
            self.conn.execute(f"DELETE FROM RETAIL_DWH.TGT.{table}")
            self.conn.execute(f"CREATE SEQUENCE RETAIL_DWH.TGT.seq_{table}")
            self.conn.execute(f"ALTER TABLE RETAIL_DWH.TGT.{table} ALTER {sk} SET DEFAULT nextval('RETAIL_DWH.TGT.seq_{table}')")
            timestamp = "UPDATED_AT" if table in ("DIM_SUBCLASS", "DIM_DISTRICT") else "REC_UPDT_TS"
            self.conn.execute(f"ALTER TABLE RETAIL_DWH.TGT.{table} ADD COLUMN {timestamp} TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        self.conn.execute("""CREATE TABLE RETAIL_DWH.TMP.INVENTORY_SNAPSHOT (
            STORE_ID VARCHAR, ITEM_ID VARCHAR, SNAPSHOT_DATE DATE, QUANTITY_ON_HAND BIGINT,
            UNIT_COST DECIMAL(18,2), INVENTORY_VALUE DECIMAL(18,2), SOURCE_ROW_ID VARCHAR)""")
        self.inventory = {}
        for path in (ROOT / "data").glob("inventory_snapshot_*.csv"):
            with path.open(newline="") as stream:
                self.inventory[path.stem[-8:]] = [
                    (r["store_id"], r["item_id"], r["snapshot_date"], r["quantity_on_hand"],
                     r["unit_cost"], r["inventory_value"], f"{path.name}:{i}")
                    for i, r in enumerate(csv.DictReader(stream), 1)]
        self.conn.execute("""CREATE TABLE RETAIL_DWH.TMP.POS_SALES (
            SALE_DATE DATE, TRANSACTION_ID VARCHAR, LINE_NUMBER BIGINT,
            STORE_ID VARCHAR, ITEM_ID VARCHAR, QUANTITY BIGINT, UNIT_PRICE DECIMAL(18,2),
            DISCOUNT_AMOUNT DECIMAL(18,2), NET_SALES DECIMAL(18,2), SOURCE_ROW_ID VARCHAR)""")
        self.sales = {}
        eligible_items = {r[0] for r in self.conn.execute("SELECT ITEM_ID FROM RETAIL_DWH.TMP.ALL_ITEM_MASTER").fetchall()}
        seen = set()
        for path in sorted((ROOT / "data").glob("pos_sales_*.csv")):
            accepted = []
            with path.open(newline="") as stream:
                for i, row in enumerate(csv.DictReader(stream), 1):
                    key = (row["transaction_id"], row["line_number"])
                    if row["item_id"] not in eligible_items or int(row["quantity"]) <= 0 or key in seen:
                        continue
                    seen.add(key)
                    accepted.append(tuple(row[field] for field in (
                        "sale_date", "transaction_id", "line_number", "store_id", "item_id",
                        "quantity", "unit_price", "discount_amount", "net_sales")) + (f"{path.name}:{i}",))
            self.sales[path.stem[-8:]] = accepted
        self.conn.execute("CREATE SEQUENCE RETAIL_DWH.TGT.seq_sales")
        self.conn.execute("""CREATE TABLE RETAIL_DWH.TGT.FCT_SALES (
            SALES_LINE_SK BIGINT DEFAULT nextval('RETAIL_DWH.TGT.seq_sales'),
            TRANSACTION_ID VARCHAR, LINE_NUMBER BIGINT, STORE_ID VARCHAR, ITEM_ID VARCHAR,
            SALE_DATE DATE, ITEM_SK BIGINT, LOCN_SK BIGINT, QUANTITY BIGINT,
            UNIT_PRICE DECIMAL(18,2), DISCOUNT_AMOUNT DECIMAL(18,2), NET_SALES DECIMAL(18,2),
            SOURCE_ROW_ID VARCHAR, REC_UPDT_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        self.conn.execute("CREATE SEQUENCE RETAIL_DWH.TGT.seq_inventory")
        self.conn.execute("""CREATE TABLE RETAIL_DWH.TGT.FCT_INVENTORY (
            INVENTORY_VERSION_SK BIGINT DEFAULT nextval('RETAIL_DWH.TGT.seq_inventory'),
            STORE_ID VARCHAR, ITEM_ID VARCHAR, ITEM_SK BIGINT, LOCN_SK BIGINT,
            QUANTITY_ON_HAND BIGINT, UNIT_COST DECIMAL(18,2), INVENTORY_VALUE DECIMAL(18,2),
            EFFECTIVE_FROM DATE, EFFECTIVE_TO DATE, IS_CURRENT BOOLEAN, SOURCE_ROW_ID VARCHAR,
            REC_UPDT_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    def run_gold(self, day):
        for filename in ("load_dim_subclass.sql", "load_dim_district.sql", "prepare_dim_item.sql",
                         "merge_dim_item.sql", "prepare_dim_locn.sql", "merge_dim_locn.sql",
                         "prepare_fct_sales.sql", "merge_fct_sales.sql",
                         "resolve_inventory_observations.sql", "prepare_fct_inventory.sql", "merge_fct_inventory.sql"):
            sql = (ROOT / "include/sql/gold" / filename).read_text().strip().rstrip(";")
            sql = scoped_gold_sql(sql, filename, day).replace("TEMPORARY TABLE", "TABLE").replace("CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP")
            if "UPDATE SET" in sql:
                before, update = sql.split("UPDATE SET", 1)
                update, after = update.split("WHEN NOT MATCHED", 1)
                sql = before + "UPDATE SET" + re.sub(r"\bt\.([A-Z_]+)\s*=", r"\1 =", update) + "WHEN NOT MATCHED" + after
            self.conn.execute(sql)

    def snapshot(self):
        return {name: self.conn.execute(f"SELECT * FROM RETAIL_DWH.TGT.{name} ORDER BY 1").fetchall()
                for name in ("DIM_SUBCLASS", "DIM_DISTRICT", "DIM_ITEM", "DIM_LOCN", "FCT_SALES", "FCT_INVENTORY")}

    def test_five_days_publish_history_incrementally_and_replay_preserves_it(self):
        for number in range(9, 14):
            day = f"2026-11-{number:02d}"
            for table in ("ITEM_MASTER", "STORE_MASTER"):
                self.conn.execute(f"INSERT INTO RETAIL_DWH.TMP.{table} SELECT * FROM RETAIL_DWH.TMP.ALL_{table} WHERE SNAPSHOT_DATE=?", [day])
            self.conn.executemany("INSERT INTO RETAIL_DWH.TMP.INVENTORY_SNAPSHOT VALUES (?,?,?,?,?,?,?)",
                                  self.inventory[day.replace("-", "")])
            self.conn.executemany("INSERT INTO RETAIL_DWH.TMP.POS_SALES VALUES (?,?,?,?,?,?,?,?,?,?)",
                                  self.sales[day.replace("-", "")])
            self.run_gold(day)
            self.assertEqual(self.conn.execute("SELECT MAX(EFFECTIVE_FROM) FROM RETAIL_DWH.TGT.FCT_INVENTORY").fetchone()[0].isoformat(), day)
            before = self.snapshot()
            self.run_gold(day)
            self.assertEqual(before, self.snapshot(), f"Replay changed keys/values/timestamps on {day}")
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM RETAIL_DWH.TGT.DIM_ITEM").fetchone()[0], 31)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM RETAIL_DWH.TGT.DIM_LOCN").fetchone()[0], 31)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM RETAIL_DWH.TGT.FCT_INVENTORY").fetchone()[0], 98)
        sales_count, sales_amount = self.conn.execute("SELECT COUNT(*), SUM(NET_SALES) FROM RETAIL_DWH.TGT.FCT_SALES").fetchone()
        self.assertEqual(sales_count, 145)
        self.assertEqual(str(sales_amount), "1566.15")
        invalid_history = self.conn.execute("""SELECT COUNT(*) FROM RETAIL_DWH.TGT.FCT_SALES f
            LEFT JOIN RETAIL_DWH.TGT.DIM_ITEM i ON i.ITEM_SK=f.ITEM_SK AND i.ITEM_ID=f.ITEM_ID
                AND i.EFFECTIVE_FROM<=f.SALE_DATE AND (f.SALE_DATE<i.EFFECTIVE_TO OR i.EFFECTIVE_TO IS NULL)
            LEFT JOIN RETAIL_DWH.TGT.DIM_LOCN l ON l.LOCN_SK=f.LOCN_SK AND l.STORE_ID=f.STORE_ID
                AND l.EFFECTIVE_FROM<=f.SALE_DATE AND (f.SALE_DATE<l.EFFECTIVE_TO OR l.EFFECTIVE_TO IS NULL)
            WHERE i.ITEM_SK IS NULL OR l.LOCN_SK IS NULL""").fetchone()[0]
        self.assertEqual(invalid_history, 0)
        self.assertEqual(self.conn.execute("SELECT SUBCLASS_NAME FROM RETAIL_DWH.TGT.DIM_SUBCLASS WHERE SUBCLASS_ID='SC03'").fetchone()[0], "Home Care")
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM RETAIL_DWH.TGT.DIM_LOCN WHERE STORE_ID='STORE007'").fetchone()[0], 2)
        # Reproduce native backfill AFTER a previous full bulk load: reprocess
        # older dates while later accepted snapshots/Gold versions already exist.
        # Full snapshots include generated keys, timestamps, intervals and names.
        baseline = self.snapshot()
        for number in range(9, 14):
            self.run_gold(f"2026-11-{number:02d}")
            self.assertEqual(baseline, self.snapshot(), f"Earlier replay regressed later history on day {number}")


if __name__ == "__main__":
    unittest.main()
