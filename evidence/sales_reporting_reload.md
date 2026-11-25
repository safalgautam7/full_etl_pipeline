# Sales reporting mart and development reload

## Implemented contract

The source folder already contained 2,328 files covering 2024-11-09 through
2026-11-18. Do not mistake the earlier ten-day warehouse load for the available
input history. The complete unchanged fixture has 138,406 raw records.

`scripts/enrich_reporting_inputs.py` adds these source attributes:

- Item master: department_id, department_name, class_id, class_name.
- Store master: location_type.

Mappings are **synthetic demonstration classifications**, defined in
`config/sales_reporting_mappings.json`, not authoritative business data.
There are four departments, six classes and three location types. Existing
fields and all sales/inventory CSV bytes are preserved. Master hashes and
matching manifest/ledger entries change deliberately. Ordinary ingestion
must not mix those new versions with old warehouse manifest identities;
this rollout uses an explicitly backed-up development reset.

The attributes pass through Silver parsing/required-field checks, typed TMP
tables, conditional MERGEs and Gold SCD2 dimensions. Changes participate in
SCD2 hashing. Merely adding CSV headers would not publish the attributes.

## New Snowflake objects

Apply after the existing warehouse DDL:

1. `include/sql/ddl/02_sales_reporting_attributes.sql`: additive columns on
   populated TMP/TGT tables. Legacy rows default to UNMAPPED until repair/reload;
   they are not valid final mart data.
2. `include/sql/ddl/03_create_sales_mart.sql`: four reporting views, with no
   independent sales copy or loading job:

```text
RETAIL_DWH.MART_SALES
  DIM_DATE
  DIM_ITEM
  DIM_LOCATION
  FCT_SALES
```

Subclass is flattened into Item; District into Location. Surrogate keys retain
SCD2 versions. Do not restrict these dimensions to current versions or join
historical facts by natural business IDs instead of their historical SKs.

The fact exposes TRANSACTION_DATE (from SALE_DATE), DATE_KEY, ITEM_SK,
LOCATION_SK, stable LOCATION_ID, TRANSACTION_ID, LINE_NUMBER, UNITS,
GROSS_SALES_AMOUNT, DISCOUNT_AMOUNT, NET_SALES_AMOUNT and audit fields.
Gross is reconstructed as accepted Net + Discount because the source has no
independent gross amount. Gross - Discount = Net is exact.
EXTENDED_PRICE_AMOUNT separately retains Quantity * Unit Price for audit;
source arithmetic permits a small rounding tolerance.

DIM_DATE includes quarter keys/labels/bounds, month attributes, ISO week-year,
week, weekday and week boundaries. PY_COMPARABLE_DATE/PY_DATE_KEY use exactly
the previous ISO week-year, same ISO week and weekday—not calendar-year date
subtraction or an unconditional 364-day shift. 2026-01-01 maps to 2025-01-02.
Missing previous-year week 53 returns NULL. ISO weeks crossing month/quarter
boundaries split under the parent month/quarter filters in the report hierarchy.

`check_sales_mart_integrity.sql` checks flattened keys, populated classifications,
orphan facts, gross/net accounting and ISO mapping. The Gold Variable enables
it with `sales_mart_enabled: true`; approved corrections also run it before COMMIT.

## Reset performed for this rollout

- Input backup: `data_backups/before_sales_reporting_20261125T051343Z/`.
- Snowflake backup database: `RETAIL_DWH_BACKUP_20261125T052216Z`.
- Full ETL run ID: `sales_reporting_reload_20261125T0523`.

The clone was made **before** migration/reset. Rows in the 22 explicitly named
pipeline tables were deleted in one DML transaction and checked empty. Table
definitions, internal-stage files, Airflow metadata and other databases were
not cleared. Identity counters are not restarted; ETL rebuilds consistent
fact/dimension references. Keep the backup until verification is complete.
Do not reset again while the reload is in progress.

The reusable maintenance command defaults to read-only preflight:

```bash
docker compose exec -T airflow-scheduler python /opt/airflow/scripts/prepare_sales_reporting_reload.py
```

Only another explicitly approved development reset should use:

```bash
docker compose exec -T airflow-scheduler python /opt/airflow/scripts/prepare_sales_reporting_reload.py --apply --confirm RESET_RETAIL_DWH
```

It refuses unpaused/outstanding retail work, source checksum mismatches, the
wrong database context and inadequate mapping capacity. It creates a fresh
backup clone before DDL/deletes. Stop external writers and BI refresh too.

## Loading and validation

2,328 inputs exceed Airflow's default max_map_length of 1024. Compose now
specifies 4096. The current installation's ignored airflow.cfg was updated to
match and existing services restarted without recreating their secrets/env.
The shared writer pool stays at one slot; map capacity is not write concurrency.

The manual full run uses file_limit=0 and creates 2,341 task instances. Files
load serially through PUT/COPY and manifest checks: expect a long reload and
Snowflake compute usage. Mart data is not ready while the DAG is in Bronze.
Standalone layer/correction DAGs remain paused. Unpausing the full daily DAG
can also create a scheduled run; missing post-Nov-18 fixture dates are not
part of the historical manual reload.

Expected final totals for this unchanged synthetic dataset, not hardcoded
production requirements:

| Result | Expected |
| --- | ---: |
| Manifest files | 2,328 |
| Raw rows across STG | 138,406 |
| Accepted item snapshots | 22,203 |
| Accepted store snapshots | 3,241 |
| Accepted / Gold sales lines | 86,652 |
| Accepted inventory / daily positions | 22,217 |
| Rejected rows across sources | 4,093 |
| Net sales | 941,254.40 |

Use Airflow logs and all four production accounting/integrity queries in
`include/sql/reconcile/`, then `evidence/verify_sales_mart.sql`. Inventory
interval counts are not daily observation counts. The 2024 opening quarter is
partial. To demonstrate the selected quarter, its preceding two quarters and
their PY comparisons completely, use **2026 Q4 or 2026 Q3**. Earlier selections
need coverage warnings or more source history; missing prior sales are not zero.

The approved demo reporting as-of is **2026-11-18**, the last covered input day,
not the machine's current date or a load/update timestamp. Both current/PY
running totals must stop at corresponding elapsed current-period dates.
Future calendar rows do not create future trading data.

## Power BI import

After the full run succeeds, import **only the four views from MART_SALES**
into a new sales model. Do not mix them with the eight-table inventory model.
Create active 1:* single-direction relationships:

- DIM_DATE.DATE_KEY -> FCT_SALES.DATE_KEY.
- DIM_ITEM.ITEM_SK -> FCT_SALES.ITEM_SK.
- DIM_LOCATION.LOCATION_SK -> FCT_SALES.LOCATION_SK.

Use matching Whole number/Don't summarize keys and mark DIM_DATE using
CALENDAR_DATE. Use dimension fields for slicers. Independent slicers require
Edit interactions settings; single-direction relationships alone do not stop
same-dimension slicers limiting one another. A disconnected quarter selector
is recommended to drive both current and preceding-two-quarter visuals.
Top-N what-if/Other helpers belong in the semantic layer.

Name the transaction measure exactly Transactioncount. Use stable LOCATION_ID
for Active Locations and like-for-like intersection, not SCD2 LOCATION_SK.
Measures, conditional formatting, calculation groups and the dashboard pages
are a subsequent report task. The mart does not create them or automatically
create Power BI relationship metadata.

## Verification boundaries

28 unit tests and two new local mart integration tests passed before reset.
They cover input preservation/backup/idempotence, history/ISO coverage, required
attributes and actual mart SQL with dialect adaptation. Airflow graph and live
preflight checks passed. Actual Snowflake DDL/reset completed successfully;
final reconciliation must wait for full ETL. Older SCD evidence tests reference
a missing verify_etl_stages.sql worksheet; do not claim that suite passed.
