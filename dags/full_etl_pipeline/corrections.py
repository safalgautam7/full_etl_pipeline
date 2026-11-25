"""Controlled full-file corrections. No Airflow dependency.

All work-table DDL precedes BEGIN. Once publication starts, only DML/SELECT
runs: raw replacement, Silver decisions, history repair, facts, and approval
activation commit together. The caller must hold the one-slot retail_writer
pool for this ENTIRE operation (including staging).
"""

import re
from pathlib import Path

from full_etl_pipeline.silver_processing import SOURCES, read_sql, validate_config

CTAS = re.compile(r"CREATE OR REPLACE TEMPORARY TABLE ([A-Z0-9_.]+) AS\s*", re.I)


def select_from_ctas(sql):
    match = CTAS.search(sql)
    if not match:
        raise ValueError("Expected a trusted temporary CTAS statement")
    return match.group(1), sql[match.end():].strip().rstrip(";")


class RepairSQL:
    def __init__(self, cursor, root):
        self.cursor = cursor
        self.root = Path(root)
        self.prepared = {}

    def text(self, layer, filename):
        return (self.root / layer / filename).read_text().strip().rstrip(";")

    def prepare(self, layer, filename):
        table, query = select_from_ctas(self.text(layer, filename))
        self.cursor.execute(f"CREATE OR REPLACE TEMPORARY TABLE {table} AS "
                            f"SELECT * FROM ({query}) repair_shape WHERE 1=0")
        self.prepared[filename] = (table, query)

    def run(self, layer, filename):
        if filename in self.prepared:
            table, query = self.prepared[filename]
            self.cursor.execute(f"DELETE FROM {table}")
            self.cursor.execute(f"INSERT INTO {table} {query}")
        else:
            self.cursor.execute(self.text(layer, filename))


def assert_checks(cursor, label):
    checks = cursor.fetchall()
    if not checks or any(int(n or 0) != 0 for _, n in checks):
        raise RuntimeError(f"{label}: {checks}")


def repair_silver(cursor, cfg, runner):
    """Revalidate ALL active history, masters first, including dependent rejects."""
    for source, definition in SOURCES.items():
        names = {**cfg["sources"][source], "quarantine": cfg["quarantine_table"]}
        runner.run("silver", definition["sql"])
        cursor.execute(read_sql(runner.root / "silver", "check_work.sql", names))
        raw, unique_raw, work, unique_work, accepted, rejected, foreign = (
            int(v or 0) for v in cursor.fetchone())
        if raw != unique_raw or raw != work or raw != unique_work or foreign or accepted + rejected != raw:
            raise RuntimeError(f"{source}: correction work coverage failed")
        # Anti-joins remove superseded rows, including deleted lines and newly
        # rejected records. Conditional MERGEs retain timestamps on other rows.
        cursor.execute(f"DELETE FROM {names['tmp']} t WHERE NOT EXISTS "
                       f"(SELECT 1 FROM {names['work']} w WHERE w.SOURCE_ROW_ID=t.SOURCE_ROW_ID "
                       "AND w.REJECT_REASON IS NULL)")
        cursor.execute(f"DELETE FROM {names['quarantine']} q WHERE q.SOURCE_TYPE=%s "
                       f"AND NOT EXISTS (SELECT 1 FROM {names['work']} w "
                       "WHERE w.SOURCE_ROW_ID=q.SOURCE_ROW_ID AND w.REJECT_REASON IS NOT NULL)", (source,))
        cursor.execute(read_sql(runner.root / "silver", f"merge_{source}.sql", names))
        cursor.execute(read_sql(runner.root / "silver", "merge_quarantine.sql", names), (source,))
        cursor.execute(read_sql(runner.root / "silver", "check_dispositions.sql", names), (source, source))
        actual = tuple(int(v or 0) for v in cursor.fetchone())
        if actual != (accepted, accepted, rejected, rejected, 0, 0, 0):
            raise RuntimeError(f"{source}: correction disposition failed: {actual}")


def delete_obsolete(cursor, table, work, keys):
    predicate = " AND ".join(f"w.{key}=t.{key}" for key in keys)
    cursor.execute(f"DELETE FROM RETAIL_DWH.TGT.{table} t WHERE NOT EXISTS "
                   f"(SELECT 1 FROM RETAIL_DWH.TMP.{work} w WHERE {predicate})")


def repair_gold(cursor, runner):
    # Preserve existing SKs at surviving business/version keys; only removed
    # boundaries disappear. New versions use the permanent table's identity.
    for lookup in ("subclass", "district"):
        runner.run("gold", f"load_dim_{lookup}.sql")
    for dimension, keys in (("item", ("ITEM_ID", "EFFECTIVE_FROM")),
                            ("locn", ("STORE_ID", "EFFECTIVE_FROM"))):
        runner.run("gold", f"prepare_dim_{dimension}.sql")
        delete_obsolete(cursor, f"DIM_{dimension.upper()}", f"WORK_DIM_{dimension.upper()}", keys)
        runner.run("gold", f"merge_dim_{dimension}.sql")
    for table, master, key in (("DIM_SUBCLASS", "ITEM_MASTER", "SUBCLASS_ID"),
                               ("DIM_DISTRICT", "STORE_MASTER", "DISTRICT_ID")):
        cursor.execute(f"DELETE FROM RETAIL_DWH.TGT.{table} t WHERE NOT EXISTS "
                       f"(SELECT 1 FROM RETAIL_DWH.TMP.{master} m WHERE m.{key}=t.{key})")

    runner.run("gold", "prepare_fct_sales.sql")
    runner.run("gold", "resolve_inventory_observations.sql")
    for table, date_column, duplicate_column in (
        ("WORK_FCT_SALES", "SALE_DATE", "BUSINESS_KEY_OCCURRENCES"),
        ("WORK_INV_OBSERVATIONS", "SNAPSHOT_DATE", "DUPLICATE_POSITION_DAY"),
    ):
        cursor.execute(f"SELECT COUNT(*) FROM RETAIL_DWH.TMP.{table} w "
                       f"WHERE ITEM_MATCH_COUNT<>1 OR LOCN_MATCH_COUNT<>1 OR {duplicate_column}<>1 "
                       "OR (SELECT COUNT(*) FROM RETAIL_DWH.TGT.DIM_DATE d "
                       f"WHERE d.CALENDAR_DATE=w.{date_column})<>1")
        if int(cursor.fetchone()[0]):
            raise RuntimeError(f"{table}: historical/calendar preflight failed")
    delete_obsolete(cursor, "FCT_SALES", "WORK_FCT_SALES", ("TRANSACTION_ID", "LINE_NUMBER"))
    runner.run("gold", "merge_fct_sales.sql")
    runner.run("gold", "prepare_fct_inventory.sql")
    delete_obsolete(cursor, "FCT_INVENTORY", "WORK_FCT_INVENTORY", ("STORE_ID", "ITEM_ID", "EFFECTIVE_FROM"))
    runner.run("gold", "merge_fct_inventory.sql")
    delete_obsolete(cursor, "FCT_INVENTORY_DAILY", "WORK_INV_OBSERVATIONS", ("STORE_ID", "ITEM_ID", "SNAPSHOT_DATE"))
    runner.run("gold", "merge_fct_inventory_daily.sql")


def correct_file(cursor, cfg, sql_root, *, filename, source, old_sha, new_sha,
                 new_file_id, correction_id, reason, approved_by, sales_mart_enabled=False):
    """REPLACEMENT_INPUT must be staged by the caller in this same session.

    Missing physical rows mean deletion from this source/date observation set;
    absent master entities are NOT automatically an SCD tombstone.
    """
    validate_config(cfg)
    if source not in SOURCES or not reason.strip() or not approved_by.strip():
        raise ValueError("Valid source, reason and approver are required")
    stg = cfg["sources"][source]["stg"]
    manifest = "RETAIL_DWH.CONTROL.FILE_MANIFEST"
    audit = "RETAIL_DWH.CONTROL.CORRECTION_RUNS"
    cursor.execute(f"SELECT SOURCE_FILE_ID, SOURCE_FILE_SHA256, ROW_COUNT, LOAD_STATUS "
                   f"FROM {manifest} WHERE SOURCE_FILE_NAME=%s", (filename,))
    rows = cursor.fetchall()
    if len(rows) != 1 or rows[0][3] != "LOADED":
        raise RuntimeError("Correction requires exactly one loaded active file")
    old_id, actual_sha, old_count, _ = rows[0]
    if actual_sha not in (old_sha, new_sha):
        raise RuntimeError("Active file changed since approval; expected checksum does not match")
    cursor.execute("SELECT COUNT(*), COUNT(DISTINCT SOURCE_ROW_ID), "
                   "COALESCE(COUNT_IF(SOURCE_FILE_ID<>%s OR SOURCE_FILE_NAME<>%s),0) "
                   "FROM RETAIL_DWH.STG.REPLACEMENT_INPUT", (new_file_id, filename))
    staged, unique_staged, foreign = (int(n or 0) for n in cursor.fetchone())
    if staged != unique_staged or foreign:
        raise RuntimeError("Replacement contains duplicate/foreign raw identities")
    if actual_sha == new_sha:
        cursor.execute(f"SELECT COUNT(*),COUNT(DISTINCT SOURCE_ROW_ID) FROM {stg} WHERE SOURCE_FILE_ID=%s",
                       (new_file_id,))
        if tuple(int(n) for n in cursor.fetchone()) != (int(old_count), int(old_count)):
            raise RuntimeError("Cannot replay correction: active raw coverage is incomplete")
    cursor.execute(f"INSERT INTO {audit} "
                   "(CORRECTION_ID,SOURCE_FILE_NAME,OLD_FILE_ID,NEW_FILE_ID,REASON,APPROVED_BY,STATUS) "
                   "VALUES (%s,%s,%s,%s,%s,%s,'RUNNING')",
                   (correction_id, filename, old_id, new_file_id, reason, approved_by))
    if actual_sha == new_sha:
        cursor.execute(f"UPDATE {audit} SET STATUS='NOOP',FINISHED_AT=CURRENT_TIMESTAMP() "
                       "WHERE CORRECTION_ID=%s", (correction_id,))
        return "NOOP"

    runner = RepairSQL(cursor, sql_root)
    try:
        # Clone SHAPES only, before BEGIN. No DDL is allowed in the repair txn.
        for definition in SOURCES.values():
            runner.prepare("silver", definition["sql"])
        for name in ("prepare_dim_item.sql", "prepare_dim_locn.sql", "prepare_fct_sales.sql",
                     "resolve_inventory_observations.sql", "prepare_fct_inventory.sql"):
            runner.prepare("gold", name)
        cursor.execute("BEGIN")
        cursor.execute(f"SELECT COUNT(*) FROM (SELECT SOURCE_FILE_NAME FROM {manifest} "
                       "GROUP BY SOURCE_FILE_NAME HAVING COUNT(*)<>1)")
        if int(cursor.fetchone()[0]):
            raise RuntimeError("Duplicate active manifest filenames; repair refused")
        # Validate the complete active raw registry, not just the changed file.
        for active_source, names in cfg["sources"].items():
            cursor.execute(f"SELECT COUNT(*) FROM {manifest} m WHERE SOURCE_TYPE=%s AND "
                           f"(LOAD_STATUS<>'LOADED' OR ROW_COUNT<>(SELECT COUNT(*) FROM {names['stg']} s "
                           "WHERE s.SOURCE_FILE_ID=m.SOURCE_FILE_ID))", (active_source,))
            if int(cursor.fetchone()[0]):
                raise RuntimeError("Active manifest/raw coverage mismatch; repair refused")
            cursor.execute(f"SELECT COUNT(*) FROM {names['stg']} s WHERE NOT EXISTS "
                           f"(SELECT 1 FROM {manifest} m WHERE m.SOURCE_FILE_ID=s.SOURCE_FILE_ID "
                           "AND m.SOURCE_TYPE=%s)", (active_source,))
            if int(cursor.fetchone()[0]):
                raise RuntimeError("Unregistered raw rows; repair refused")
            cursor.execute(f"SELECT COUNT(*)-COUNT(DISTINCT SOURCE_ROW_ID) FROM {names['stg']}")
            if int(cursor.fetchone()[0]):
                raise RuntimeError("Duplicate active raw identities; repair refused")
        cursor.execute("INSERT INTO RETAIL_DWH.CONTROL.RAW_VERSION_ARCHIVE "
                       "(CORRECTION_ID,SOURCE_TYPE,SOURCE_FILE_ID,RAW_RECORD) "
                       f"SELECT %s,%s,SOURCE_FILE_ID,OBJECT_CONSTRUCT_KEEP_NULL(s.*) FROM {stg} s "
                       "WHERE SOURCE_FILE_ID=%s", (correction_id, source, old_id))
        cursor.execute("INSERT INTO RETAIL_DWH.CONTROL.MANIFEST_VERSION_ARCHIVE "
                       "(CORRECTION_ID,MANIFEST_RECORD) "
                       f"SELECT %s,OBJECT_CONSTRUCT_KEEP_NULL(m.*) FROM {manifest} m "
                       "WHERE SOURCE_FILE_NAME=%s", (correction_id, filename))
        cursor.execute("INSERT INTO RETAIL_DWH.CONTROL.DQ_DECISION_ARCHIVE "
                       "(CORRECTION_ID,DECISION_RECORD) SELECT %s,OBJECT_CONSTRUCT_KEEP_NULL(q.*) "
                       f"FROM {cfg['quarantine_table']} q", (correction_id,))
        cursor.execute(f"DELETE FROM {stg} WHERE SOURCE_FILE_ID=%s", (old_id,))
        columns = ("SOURCE_ROW_ID,SOURCE_FILE_ID,SOURCE_FILE_NAME,SOURCE_FILE_DATE,"
                   "SOURCE_ROW_NUMBER,RAW_PAYLOAD,BATCH_ID")
        cursor.execute(f"INSERT INTO {stg} ({columns}) SELECT {columns} "
                       "FROM RETAIL_DWH.STG.REPLACEMENT_INPUT")
        cursor.execute(f"UPDATE {manifest} SET SOURCE_FILE_ID=%s,SOURCE_FILE_SHA256=%s,"
                       "ROW_COUNT=(SELECT COUNT(*) FROM RETAIL_DWH.STG.REPLACEMENT_INPUT),"
                       "BATCH_ID=%s,LOADED_AT=CURRENT_TIMESTAMP() WHERE SOURCE_FILE_NAME=%s",
                       (new_file_id, new_sha, correction_id, filename))
        repair_silver(cursor, cfg, runner)
        repair_gold(cursor, runner)
        runner.run("reconcile", "reconcile_gold.sql")
        reconciliation = cursor.fetchall()
        # An empty replaced sales day is legitimate: the registry preserves the
        # fact that the zero-row replacement exists, although no observed date
        # appears in the row-derived reconciliation query.
        if any(row[-1] != "PASS" for row in reconciliation):
            raise RuntimeError(f"Corrected sales reconciliation failed: {reconciliation}")
        for check in ("check_gold_integrity.sql", "check_reporting_integrity.sql"):
            runner.run("reconcile", check)
            assert_checks(cursor, check)
        if sales_mart_enabled:
            runner.run("reconcile", "check_sales_mart_integrity.sql")
            assert_checks(cursor, "check_sales_mart_integrity.sql")
        cursor.execute(f"UPDATE {audit} SET STATUS='SUCCEEDED',FINISHED_AT=CURRENT_TIMESTAMP() "
                       "WHERE CORRECTION_ID=%s", (correction_id,))
        cursor.execute("COMMIT")
    except Exception as exc:
        cursor.execute("ROLLBACK")
        cursor.execute(f"UPDATE {audit} SET STATUS='FAILED',FINISHED_AT=CURRENT_TIMESTAMP(),ERROR_DETAIL=%s "
                       "WHERE CORRECTION_ID=%s", (str(exc)[:4000], correction_id))
        raise
    return "SUCCEEDED"
