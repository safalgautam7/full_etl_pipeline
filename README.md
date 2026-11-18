# Stage 4 — STG Ingestion (guided build)

Prerequisites: Airflow 3.3.2 Docker stack, existing 4 empty STG tables,
`file_con`, `retail_snowflake`, `RETAIL_DWH.STG.CSV_LANDING`, 17 CSV files.

## Put files in existing project

- Copy `include/sql/control/01_file_manifest.sql` into the project, execute in a Snowflake worksheet.
- Copy `config/retail_etl.json` to `/mnt/d/bootcamp/config/retail_etl.json`. (Existing config volume is already mounted.)
- Copy `dags/full_etl_pipeline/stg_ingestion.py` to `/mnt/d/bootcamp/dags/full_etl_pipeline/stg_ingestion.py`.
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
