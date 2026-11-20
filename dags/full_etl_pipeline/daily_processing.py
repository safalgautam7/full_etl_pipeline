"""Daily scope adapters. No Airflow imports; all dates/identifiers validated.

Historical reads are permitted; publication is limited to today's rows and
closing the prior SCD/inventory intervals affected by today's new boundary.
"""

from copy import deepcopy
from datetime import date, datetime
from pathlib import Path
import hashlib
import re

from full_etl_pipeline.silver_processing import SOURCES, LINEAGE_COLS, publish_work, validate_config

FILE_PATTERN = re.compile(r"^(item_master|store_master|pos_sales|inventory_snapshot)_(\d{8})\.csv$")


def inspect_filename(filename):
    match = FILE_PATTERN.fullmatch(filename)
    if not match:
        raise ValueError(f"Unexpected CSV name: {filename}")
    source, value = match.groups()
    return source, datetime.strptime(value, "%Y%m%d").date()


def processing_day(context):
    """Native scheduled/backfill runs use their interval, never wall-clock time."""
    if context["dag"].dag_id != "retail_full_etl":
        return None
    run_type = context["dag_run"].run_type
    run_type = getattr(run_type, "value", run_type)
    if run_type not in {"scheduled", "backfill"}:
        return None  # Preserve original manually triggered bulk/layer behavior.
    return context["data_interval_start"].date().isoformat()


def business_date(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("business_date must be YYYY-MM-DD")
    return date.fromisoformat(value)


def select_day_files(filenames, day):
    day = business_date(day)
    selected = [(name, *inspect_filename(name)) for name in filenames if FILE_PATTERN.fullmatch(name)]
    selected = [(name, source) for name, source, observed in selected if observed == day]
    missing = {"item_master", "pos_sales", "inventory_snapshot"} - {source for _, source in selected}
    if missing:
        raise ValueError(f"{day}: missing required daily inputs: {sorted(missing)}")
    return sorted(name for name, _ in selected)


def latest_warehouse_date(cursor, stg_tables):
    dates = [f"SELECT SOURCE_FILE_DATE D FROM {table}" for table in stg_tables]
    dates += [f"SELECT SOURCE_FILE_DATE D FROM RETAIL_DWH.TMP.{source.upper()}" for source in SOURCES]
    for table, column in (("DIM_ITEM", "EFFECTIVE_FROM"), ("DIM_LOCN", "EFFECTIVE_FROM"),
                          ("FCT_SALES", "SALE_DATE"), ("FCT_INVENTORY", "EFFECTIVE_FROM")):
        dates.append(f"SELECT {column} D FROM RETAIL_DWH.TGT.{table}")
    cursor.execute(f"SELECT MAX(D) FROM ({' UNION ALL '.join(dates)})")
    return cursor.fetchone()[0]


def verify_replay_files(cursor, cfg, files):
    """Permit earlier-day replay only for identical fully loaded files."""
    for filename, digest in files:
        source, _ = inspect_filename(filename)
        expected_id = hashlib.sha256(f"{filename}:{digest}".encode()).hexdigest()
        cursor.execute(f"SELECT SOURCE_FILE_SHA256, ROW_COUNT, LOAD_STATUS, SOURCE_FILE_ID "
                       f"FROM {cfg['file_manifest_table']} WHERE SOURCE_FILE_NAME=%s", (filename,))
        registered = cursor.fetchall()
        if (len(registered) != 1 or registered[0][0] != digest
                or registered[0][2] != "LOADED" or registered[0][3] != expected_id):
            raise RuntimeError(f"{filename}: earlier-day replay requires the identical already-loaded file; historical inserts/corrections are unsupported")
        cursor.execute(f"SELECT COUNT(*), COUNT(DISTINCT SOURCE_ROW_ID) FROM {cfg['stg_tables'][source]} "
                       "WHERE SOURCE_FILE_ID=%s", (expected_id,))
        count, unique_count = cursor.fetchone()
        if count != registered[0][1] or unique_count != count:
            raise RuntimeError(f"{filename}: existing raw rows are incomplete or duplicated; replay refused")


def verify_replay_decisions(cursor, cfg, source):
    """Reject historical rule/disposition changes before any permanent DML."""
    names = cfg["sources"][source]
    lineage = LINEAGE_COLS.split(", ")
    accepted = lineage + SOURCES[source]["cols"].split(", ")
    accepted_changes = " OR ".join(f"t.{column} IS DISTINCT FROM w.{column}" for column in accepted)
    rejected_changes = " OR ".join(f"q.{column} IS DISTINCT FROM w.{column}" for column in lineage)
    cursor.execute(f"""SELECT COUNT(*) FROM {names['work']} w
        LEFT JOIN {names['tmp']} t ON t.SOURCE_ROW_ID=w.SOURCE_ROW_ID
        LEFT JOIN {cfg['quarantine_table']} q
          ON q.SOURCE_ROW_ID=w.SOURCE_ROW_ID AND q.SOURCE_TYPE=%s
        WHERE (w.REJECT_REASON IS NULL AND
                 (t.SOURCE_ROW_ID IS NULL OR q.SOURCE_ROW_ID IS NOT NULL OR {accepted_changes}))
           OR (w.REJECT_REASON IS NOT NULL AND
                 (q.SOURCE_ROW_ID IS NULL OR t.SOURCE_ROW_ID IS NOT NULL OR {rejected_changes}
                  OR q.REASON_CODE IS DISTINCT FROM w.REJECT_REASON
                  OR q.REASON_DETAIL IS DISTINCT FROM ('Validation failed: ' || w.REJECT_REASON)
                  OR q.RAW_PAYLOAD IS DISTINCT FROM w.RAW_PAYLOAD))""", (source,))
    if int(cursor.fetchone()[0]):
        raise RuntimeError(f"{source}: historical replay would change existing Silver/DQ decisions; explicit repair is required")


def process_daily_source(cursor, cfg, source, day, sql_dir):
    """Reuse the classifier and atomic publisher against one day's raw scope."""
    day = business_date(day).isoformat()
    validate_config(cfg)
    cfg = deepcopy(cfg)
    names = cfg["sources"][source]
    scoped = f"RETAIL_DWH.TMP.DAILY_RAW_{source.upper()}"
    cursor.execute(f"CREATE OR REPLACE TEMPORARY TABLE {scoped} AS "
                   f"SELECT * FROM {names['stg']} WHERE SOURCE_FILE_DATE = '{day}'::DATE")
    cursor.execute(f"SELECT COUNT(*) FROM {scoped}")
    if int(cursor.fetchone()[0]) == 0:
        if source == "store_master":
            print(f"No store snapshot on {day}; reuse prior accepted snapshot")
            return
        raise RuntimeError(f"{source}: no raw rows for {day}")
    sql = (Path(sql_dir) / SOURCES[source]["sql"]).read_text().strip().rstrip(";")
    original = f"FROM RETAIL_DWH.STG.{source.upper()} s"
    if sql.count(original) != 1:
        raise RuntimeError("Classifier source anchor changed; review daily adapter")
    sql = sql.replace(original, f"FROM {scoped} s")
    if source == "pos_sales":
        # The business key is global, not per day. Prior accepted lines win.
        sql = sql.replace("WHEN BUSINESS_KEY_RN > 1 THEN", f"""WHEN BUSINESS_KEY_RN > 1 OR EXISTS (
            SELECT 1 FROM {names['tmp']} previous
            WHERE previous.TRANSACTION_ID = r.TRANSACTION_ID
              AND previous.LINE_NUMBER = r.LINE_NUMBER
              AND previous.SALE_DATE < '{day}'::DATE) THEN""")
    cursor.execute(sql)
    latest = latest_warehouse_date(cursor, [item["stg"] for item in cfg["sources"].values()])
    if latest and latest.isoformat() > day:
        verify_replay_decisions(cursor, cfg, source)
    names["stg"] = scoped
    return publish_work(cursor, cfg, source, sql_dir)


def scoped_gold_sql(sql, filename, day):
    """Transform trusted checked-in SQL, failing closed on changed anchors."""
    day = business_date(day).isoformat()
    bound = f"'{day}'::DATE"
    if filename in ("load_dim_subclass.sql", "load_dim_district.sql"):
        table = "ITEM_MASTER" if filename == "load_dim_subclass.sql" else "STORE_MASTER"
        anchor = f"FROM RETAIL_DWH.TMP.{table}\n"
        if sql.count(anchor) != 1:
            raise RuntimeError("SCD1 source anchor changed")
        key = "SUBCLASS_ID" if table == "ITEM_MASTER" else "DISTRICT_ID"
        # Restrict the affected keys to today, but choose their latest accepted
        # description globally. Replaying Nov 9 must not undo a Nov 13 rename.
        sql = sql.replace(anchor, anchor + f"    WHERE {key} IN (SELECT {key} FROM RETAIL_DWH.TMP.{table} "
                          f"WHERE SNAPSHOT_DATE = {bound})\n")
    # History preparation intentionally reads ALL accepted snapshots. On a
    # previously bulk-loaded target, this preserves later interval boundaries
    # and current flags instead of reopening old versions during daily replay.
    elif filename == "prepare_fct_sales.sql":
        anchor = "FROM RETAIL_DWH.TMP.POS_SALES s"
        if sql.count(anchor) != 3:
            raise RuntimeError("Sales source anchors changed")
        sql = sql.replace(anchor, f"FROM (SELECT * FROM RETAIL_DWH.TMP.POS_SALES WHERE SALE_DATE = {bound}) s")
    elif filename == "merge_fct_inventory_daily.sql":
        anchor = "USING RETAIL_DWH.TMP.WORK_INV_OBSERVATIONS s"
        if sql.count(anchor) != 1:
            raise RuntimeError("Daily inventory MERGE source anchor changed")
        sql = sql.replace(anchor, "USING (SELECT * FROM RETAIL_DWH.TMP.WORK_INV_OBSERVATIONS "
                          f"WHERE SNAPSHOT_DATE = {bound}) s")
    elif filename in ("merge_dim_item.sql", "merge_dim_locn.sql", "merge_fct_inventory.sql"):
        work = {"merge_dim_item.sql": "WORK_DIM_ITEM", "merge_dim_locn.sql": "WORK_DIM_LOCN",
                "merge_fct_inventory.sql": "WORK_FCT_INVENTORY"}[filename]
        anchor = f"USING RETAIL_DWH.TMP.{work} s"
        if sql.count(anchor) != 1:
            raise RuntimeError("History MERGE source anchor changed")
        predicate = f"EFFECTIVE_FROM = {bound} OR EFFECTIVE_TO = {bound}"
        sql = sql.replace(anchor, f"USING (SELECT * FROM RETAIL_DWH.TMP.{work} WHERE {predicate}) s")
    elif filename == "reconcile_gold.sql":
        sql = f"SELECT * FROM ({sql.strip().rstrip(';')}) daily_reconciliation WHERE BUSINESS_DATE = {bound}"
    return sql
