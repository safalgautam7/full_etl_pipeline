# Synthetic historical input

Generate with `python3 scripts/generate_retail_history.py` from the repository root.
No Airflow/Snowflake connection is made. Existing CSVs are never overwritten.
The default range is **2024-11-09 through 2026-11-08**, inclusive (730 dates).
The original November 9–18, 2026 fixtures remain unchanged.

Optional monthly batches use `--start YYYY-MM-DD --end YYYY-MM-DD`. Use the same
seed for every batch; the default is 20261109. Weekly store snapshots follow a
fixed cadence from November 9, 2024, not the beginning of each batch. Load the
complete historical dataset only after all batches are generated and validated.

## Data and expectations

- Complete daily item snapshots and weekly store snapshots retain the original
  30 item/store IDs. Bad master rows are extras with separate natural keys.
- Daily sales have deterministic random variation, weekend/seasonal effects,
  and an 8-percentage-point annual volume factor from the 2024 baseline.
- Inventory contains 30 valid paired store/item daily positions. Replenishment
  is simplified; this is not full stock-flow accounting or every store/item pair.
- Daily defects rotate across zero/negative quantities, amount mismatch and
  unknown references. Sales and inventory also contain one canonical duplicate
  copy rejected by the existing SQL; master files contain invalid extras.
- This is synthetic demonstration data, not real historical evidence.
- `data/file_manifest.json` covers every recognized CSV, including original
  fixtures. Its version label identifies the combined dataset. Checksums and
  row totals are refreshed without changing the manifest structure.
- `evidence/synthetic_history_expected.json` identifies generator version/seed
  and expected outcomes **only for newly generated historical files**. It records
  rejected physical row numbers (header excluded), reasons, row counts, monetary
  totals and calendar-quarter accepted sales totals in integer cents.

Compare the ledger against actual pipeline decisions by filename/row number.
Expected amounts obey raw = accepted + rejected; numeric defect values are kept
parseable for this accounting. Accepted sales should match Gold sales keys and
amounts. Compare inventory daily positions against `FCT_INVENTORY_DAILY`, not
the change-interval table. Existing fixture expectations remain in tests/fixtures;
whole-warehouse totals must include those fixtures, not just this ledger.

## Loading and calendar

No pipeline or calendar SQL was changed. Manual `retail_full_etl` with
`file_limit=0` discovers all files, including older dates. Its scheduling start
date still prevents native daily backfills before November 9, 2026.
Before triggering, assess existing warehouse history and Airflow mapping capacity
(the expanded folder has over 2,000 CSVs). No load or reset is performed by the
generator. Do not assume ordinary ETL safely prepends history to a populated
warehouse; prefer an approved empty development environment for initialization.

The existing DIM_DATE loader retains DAYOFWEEKISO/WEEKISO/YEAROFWEEKISO.
Verify contiguous date coverage and New Year ISO week transitions after loading.
Quarter totals use calendar quarters; weekly analysis must use ISO week year and
ISO week number together. The first and last quarters are partial. In particular,
November/December 2024 and January 2025 exercise the ISO year boundary.

Adding history intentionally expands the fixture manifest: tests that assert
exactly 33 files or original global totals may no longer apply to the combined
dataset. Original CSVs, expected fixture files and pipeline tests are not edited.
