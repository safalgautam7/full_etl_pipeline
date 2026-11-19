# Retail ETL — Airflow / Snowflake

Airflow 3.3.2 in Docker/WSL2 loads CSV files into **STG (Bronze)**, validates
them into **TMP (Silver)** or **DQ**, then loads **TGT (Gold)** dimensions and
facts. Reconciliation failures fail the overall DAG.

## Current entry points

- `retail_full_etl`: recommended full-pipeline entry point. Its expandable
  `run_stg`, `run_tmp`, and `run_gold` TaskGroups reuse the existing tasks.
  Default `{"file_limit": 0}` selects all recognized CSVs.
- `retail_stg_ingestion`: standalone ingestion; default `file_limit=1` is a smoke test.
- `retail_tmp_processing`: standalone Silver validation/publication after STG.
- `retail_gold_pipeline`: standalone dimensions, facts, and reconciliation after TMP.
- `retail_tgt_dimensions`: older dimensions-only entry point; not needed for a full run.

**Do not run these entry points concurrently.** Per-DAG concurrency is one,
but does not prevent different DAGs from publishing the same shared tables.
The full DAG runs its own tasks, not separate child DAG runs. STG `BATCH_ID`
records the run that first ingested a file; skipped replay retains that lineage.

Existing connections: `file_con` (landing directory `/opt/airflow/data`) and
`retail_snowflake`. STG/TMP use `config/retail_etl.json` and `config/retail_tmp.json`.
Gold reads the `retail_gold_config` Airflow Variable; its non-secret source is
`config/retail_gold_config.json`. Never commit connection credentials.

## Silver conditional MERGE checkpoint

`dags/full_etl_pipeline/silver_processing.py` runs the original four classifiers,
then publishes using separate SQL files in `include/sql/silver/`:

1. Build the session-local decision table **before** the DML transaction.
2. Check physical identity uniqueness and exact STG/work coverage.
3. Remove only dispositions that change accepted↔rejected.
4. Merge accepted rows on `SOURCE_ROW_ID`, and rejected rows on
   `(SOURCE_TYPE, SOURCE_ROW_ID)`, with complete null-safe changed-value checks.
5. Verify counts, classification, and exclusivity before commit; rollback on error.

Identical replay does not rewrite TMP/DQ, preserving `PROCESSED_AT` and
`REJECTED_AT`. Raw STG rows and IDs are never updated. DQ is a **current
disposition** table, not append-only decision history. No DDL migration is needed.

**Not yet completed:** date-scoped five-day parameters/publication, complete input
readiness sensors, cross-DAG locking, logical batch/day audit, persisted reconciliation,
and full Snowflake acceptance evidence. Discovery still selects all recognized files;
having five fixture dates on disk is not a date-scoped backfill feature.
Do not change validation rules on working data without downstream Gold repair:
the current Gold merge does not remove sales lines that become rejected.
Same-name changed files remain deliberately rejected (Policy A); versioned
corrections and aggregates are separate optional enhancements.

## Safe local tests

Run from the project root. Unit tests need only Python; local SQL regression
tests require DuckDB 1.4 or newer and use disposable in-memory tables only:

```bash
PYTHONPATH=dags python3 -m unittest discover -s tests/unit -v
PYTHONPATH=dags uv run --with 'duckdb>=1.4,<2' --no-project python -m unittest discover -s tests/integration -v
docker compose exec -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor python /opt/airflow/scripts/validate_retail_dags.py
docker compose exec airflow-scheduler airflow dags list-import-errors --output json
```

Alternatively, install `requirements-test.txt` in a test virtual environment and
use its Python for integration discovery. DuckDB verifies publication SQL,
conditional updates, exact replay, dispositions, duplicate protection, and
rollback. It does **not** establish Snowflake-specific classifier/VARIANT behavior.
Tests deliberately change synthetic data, never the original 17 CSVs or working warehouse.

### Opt-in isolated Snowflake test

The real fixture/replay test is skipped by default. Before enabling it, an
authorized developer must provide an **existing disposable database** named
`RETAIL_ETL_TEST_*`, a warehouse, and a role permitted to create schemas there.
Keep a Snowflake Python connector JSON configuration **outside this repository**
(fields such as `account`, `user`, `authenticator`/credentials, `warehouse`,
`role`, and `database`). No configuration file or credentials are supplied here.

```bash
RETAIL_TEST_ALLOW_WRITES=1 RETAIL_TEST_SNOWFLAKE_CONFIG=/absolute/private/connection.json PYTHONPATH=dags uv run --with snowflake-connector-python --no-project python -m unittest discover -s tests/integration -p test_snowflake_silver.py -v
```

Enabling this explicitly authorizes creation and cleanup of three uniquely named
test schemas in that test database. It never creates a database, never targets
`RETAIL_DWH`, rewrites only temporary test copies of SQL, and drops only schemas
created by that invocation. The test loads the fixture into isolated STG, runs
the real four classifiers/publication twice, asserts 505 accepted / 5 rejected
with exact reasons, and compares all STG/TMP/DQ columns including timestamps.
It does not test Gold or substitute for the remaining full acceptance catalog.

See `evidence/silver_merge_checkpoint.md` for the actual verification status.

## Standalone STG reference

Prerequisites: Airflow 3.3.2 Docker stack, existing 4 STG tables,
`file_con`, `retail_snowflake`, `RETAIL_DWH.STG.CSV_LANDING`, 17 CSV files.

## Put files in existing project

- Copy `include/sql/control/01_file_manifest.sql` into the project, execute in a Snowflake worksheet.
- Configuration belongs under this project's `config/`, mounted at `/opt/airflow/config`.
- DAGs belong under this project's `dags/`, mounted at `/opt/airflow/dags`.
- Do not overwrite the existing connection-test DAGs, source CSVs, or 4 STG tables.

## Execution

1. Trigger `retail_stg_ingestion` with default `{"file_limit": 1}`.
   Sorted filenames mean the first file is `inventory_snapshot_20261109.csv`.
   Verify STG inventory count = 30, manifest row count = 1.
2. Trigger with JSON config `{"file_limit": 0}`.
   It processes all recognized CSVs (17 for current fixture). Already loaded files SKIP.
3. Trigger with `{"file_limit": 0}` a second time; all should SKIP and totals remain identical.

## Snowflake verification

```sql
SELECT 'ITEM_MASTER' AS TABLE_NAME, COUNT(*) AS N FROM RETAIL_DWH.STG.ITEM_MASTER
UNION ALL SELECT 'STORE_MASTER', COUNT(*) FROM RETAIL_DWH.STG.STORE_MASTER
UNION ALL SELECT 'POS_SALES', COUNT(*) FROM RETAIL_DWH.STG.POS_SALES
UNION ALL SELECT 'INVENTORY_SNAPSHOT', COUNT(*) FROM RETAIL_DWH.STG.INVENTORY_SNAPSHOT;
-- Expected after first full run: 150, 60, 150, 150 (510 total).

SELECT SOURCE_FILE_NAME, ROW_COUNT, LOAD_STATUS
FROM RETAIL_DWH.CONTROL.FILE_MANIFEST ORDER BY SOURCE_FILE_NAME;
-- Expected: 17 filenames, each ROW_COUNT=30, LOAD_STATUS=LOADED.

SELECT COUNT(*) AS NULL_ITEM_ID_IN_RAW
FROM RETAIL_DWH.STG.POS_SALES
WHERE SOURCE_FILE_DATE='2026-11-11'
  AND NULLIF(TRIM(RAW_PAYLOAD:item_id::VARCHAR),'') IS NULL;
-- Expected: 2 (raw defects preserved, not quarantined here).

SELECT COUNT(DISTINCT SOURCE_ROW_ID) FROM RETAIL_DWH.STG.POS_SALES;
-- Expected: 150.
```

## Behavior and limitations

- `file_limit` is a temporary learning/smoke-test option. `0` means every recognized CSV.
- `max_active_tasks=1` serializes dynamic mapped tasks to simplify safe first loads.
  With more sophisticated orchestration, implement transactional file claims and concurrency tests.
- Same filename with different content fails closed and needs an explicit correction/backfill policy.
- Invalid *business* rows (negative quantity, blank item ID) are preserved in STG.
- Physically malformed CSV (duplicate headers, mismatched row width, invalid quoting) fails the file;
  the original CSV is never deleted and can be remediated. Production needs a structural
  ingestion quarantine/audit workflow for these failures.
- Test oracle `tests/fixtures/expected_results.json` is never read by this DAG.
- Schema names in the temporary buffer DDL are currently project-specific;
  refactor them into configuration if you deploy into another database/schema.
- Snowflake standard-table unique constraints are not enforcement mechanisms; this initial
  DAG prevents concurrent runs/tasks, checks manifest identities and uses MERGE.
