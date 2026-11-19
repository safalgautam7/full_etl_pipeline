# Silver MERGE checkpoint — 2026-11-19

## Scope and baseline

Baseline source revision: `3ed06960bdb34cefbfee8ca685c883f75f1a51da`.
The working tree was clean before this checkpoint. No live Snowflake baseline
snapshot, DESCRIBE, ETL run, or warehouse mutation was performed by this change.
The evidence below distinguishes existing run logs from newly executed tests.

Existing full-pipeline run:
`manual__2026-11-19T10:31:16.207242+00:00` under
`logs/dag_id=retail_full_etl/`.

- Landing files checked locally: 17 CSVs / 510 rows
  (item 150, store 60, sales 150, inventory 150).
- Existing logs: 17 mapped physical ingestion task logs.
- Existing Silver logs: 505 accepted and 5 quarantined rows.
- Existing fact preflight: 145 sales rows; 150 inventory observations, 98 versions.
- Existing reconciliation log: five daily PASS results, accepted/Gold total 1566.15,
  rejected sales amount 61.70; all 13 integrity checks report zero failures.

These are historical execution observations, **not** fresh warehouse assertions
or proof that the new publication code has run in Snowflake.

## New verification actually executed

| Check | Result |
|---|---|
| `PYTHONPATH=dags python3 -m unittest discover -s tests/unit -v` | 8 passed |
| Local integration discovery with DuckDB | 11 passed; 1 Snowflake test skipped |
| `scripts/validate_retail_dags.py` inside Airflow 3.3.2 | PASS |
| `airflow dags list-import-errors --output json` | `[]` |
| `git diff --check` | PASS |
| Ruff on changed Python modules, tests, and DAG validation script | PASS |

Local SQL tests execute the actual publication files with disposable in-memory
tables. They cover exact timestamp-preserving replay for all four sources,
new rows, every business-column update, nullable-value changes, rejected reason
and payload changes, both reclassification directions, rollback after targeted
deletes and a merge, duplicate work/target identities, wrong work identities,
and quarantine isolation between source types. Snowflake-specific DDL type
names and connector placeholders are adapted in the local test harness only.

No source CSV, classification SQL, STG loading logic, Gold loading SQL, connection,
or populated warehouse table was modified as part of this checkpoint.

## Not verified / next gate

The new opt-in Snowflake test has **not run**. Supply an existing disposable
`RETAIL_ETL_TEST_*` database and private connector configuration, then run the
README command. Do not equate the local SQL pass with Snowflake acceptance.
The date-scoping/readiness/audit checkpoint and full Gold replay/negative tests
remain pending; this artifact is not final assignment sign-off.
