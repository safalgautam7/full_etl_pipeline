# Assignment 2.2: Architecture notes

**Status:** Design only. This document describes intended behavior, not implemented tests or pipelines.  
 **Diagram:** `architecture.mmd`

## 1. Data flow and layer contracts

`Source files -> G0 -> Bronze (STG) -> G1 -> Silver (TMP) -> G2 -> Gold (TGT) -> Gold-owned mart -> G3 -> consumers`

| Stage | Input | Transformation and output | Guarantees | Does not guarantee |
|---|---|---|---|---|
| **Bronze / STG** | Original RMS, POS, OMS, WMS, PIM, store-ops, RPM, RPAS, finance and manifest files, in CSV/PSV formats | Store original files unchanged; record source, checksum, batch, arrival time and source period. Output: immutable/replayable landing data and provenance. | Traceability to an exact source extract; same bytes can be replayed. | Correct values, consistent schemas, unique business records or normalized keys. |
| **Silver / TMP** | Bronze files that pass ingestion checks | Type and date normalization; return/void/discount handling; item and location crosswalks; units and price normalization; fiscal-date checks; conflict detection. Output: conformed staging tables and exception records. | Accepted records have typed fields, source lineage and specified canonical business keys. | Every source conflict is automatically resolvable; warehouse surrogate keys or a published business fact are available. |
| **Gold / TGT** | Accepted Silver records and approved reference mappings | Build SCD1/SCD2 dimensions; resolve time-correct keys; upsert facts at their defined grains. Output: 9 DIM and 3 FCT tables from the 2.1 ERD. | Published records meet uniqueness, referential-integrity, history and grain rules stated below. | Source truth beyond the available data, perfectly reconciling inventory, or fields not supplied by any extract. |
| **Mart (Gold-owned)** | Published `FCT_SALES` and its dimensions | Aggregate sales by item, location and day. Output: `AGG_SALES_ITEM_LOCN_DAY`. | Published measures reconcile to eligible Gold detail for the same filters and date range. | Transaction-line detail or an independent fourth medallion layer. |

**Reference-architecture decision:** The example drawing ends with a distinct *Marts* box, but the aggregate is owned and governed as a **Gold** object, not as a fourth data-quality layer. This follows the 2.1 ERD, which already defines `AGG_SALES_ITEM_LOCN_DAY`, and avoids two conflicting definitions of “Gold.”

## 2. Source feeds and target objects

All **13 objects** in the 2.1 ERD are owned by **Gold**. Staging is separate and does not create duplicate copies of those named warehouse objects.

| Source inputs (as listed in the catalog) | Key Silver transformation | Gold outcome |
|---|---|---|
| RMS `DEPS`, `CLASS`, `SUBCLASS`, `ITEM_MASTER`, `ITEM_MASTER_HIST`, `ITEM_UPC`, `PACKITEM`; PIM `STYLE_EXPORT` | Select a canonical RMS hierarchy; map alternate item/UPC/style/pack IDs; interpret item history and case-pack factors. Do not silently substitute the different PIM tree. | `DIM_DEPT`, `DIM_CATG`, `DIM_SUBCATG`, `DIM_ITEM` (item SCD2) |
| RMS `CHAIN`, `AREA`, `REGION`, `DISTRICT`, `STORE`, `WH`, `STORE_DISTRICT_MOVE`; store-ops `LOCATION_ROSTER` | Canonicalize store, WEB and DC identifiers; resolve organization names; apply the 2026-09-15 store move as dated history where supported. | `DIM_REGN`, `DIM_CITY`, `DIM_LOCN` (location SCD2) |
| Finance `FISCAL_CALENDAR_SEED` | Validate the Sunday-based 4-5-4 calendar; reconcile missing, duplicate and inconsistent seed dates using approved corrections (not ISO-week assumptions). | `DIM_DAY` (seeded) |
| Xstore `SA_TRAN_ITEM`, NCR `NCR_SALES_JOURNAL`, OMS `WEB_ORDER_LINES` | Standardize timestamps, source-specific returns, voids, discounts, currency and money units; compose durable transaction-line identities; map item and location IDs. | `FCT_SALES`; `DIM_CUSTOMER` only where source identity is available |
| RMS `ITEM_LOC_SOH_FULL_*`, `TRAN_DATA_*`; WMS `DC_ONHAND_SNAPSHOT`, `DC_MOVEMENT_DELTA` | Normalize signs and units; convert cases to eaches using `PACKITEM`; compare snapshots with movements; establish valid-from/to inventory positions. | `FCT_INVENTORY` |
| RPM `PRICE_ZONE_LOC`, `PRICE_EVENTS` | Expand approved effective-dated price events to item/location/day; normalize cents versus money amounts; resolve zone and event overlaps deterministically. | `FCT_PRICE` |
| Published `FCT_SALES` | Group by item, location and day; calculate quantity, sales amounts, transaction count and line count. | `AGG_SALES_ITEM_LOCN_DAY` |

The catalog also contains `RPAS/DEMAND_FORECAST_WK.csv`. **Keep it in Bronze** for traceability; a demand-forecast fact is **not defined in 2.1**, so do not invent a new Gold object for Assignment 2.2. The catalog does not identify a separate customer master or a verified city source. `DIM_CUSTOMER` needs an agreed anonymous/unknown member for sales without customer IDs, and `DIM_CITY` must wait for a verified city mapping if the roster or store feed does not supply one. Do not invent city or customer values.

The 2.1 model is a selected projection of the richer source hierarchies: RMS organization has chain, area, region and district, while the selected dimensions are region, city and location. Likewise the source product hierarchy has more levels than `DIM_DEPT -> DIM_CATG -> DIM_SUBCATG -> DIM_ITEM`. This mapping needs documented business approval; absent levels are not silently treated as equivalent.

## 3. Named quality gates and failure actions

**FAIL** = block the affected file, batch or publication; **QUARANTINE** = retain bad rows outside the accepted output with file, line, reason and batch; **ALERT** = log a reviewed discrepancy and notify an owner without automatically altering data. A gate can produce more than one action.

| Rule | Boundary | Validation | Failure action |
|---|---|---|---|
| **G0.1** | Source -> Bronze | Required files and manifest entries match the declared batch; check file identity/checksum and detect accidental replays. | **FAIL** an incomplete or conflicting mandatory batch; safely skip an identical already-landed file. |
| **G0.2** | Source -> Bronze | CSV/PSV parse, encoding, embedded NCR headers, and Xstore body/trailer counts; retain original bytes regardless of any issue. | **QUARANTINE** malformed/header rows; **ALERT** on trailer mismatch; **FAIL** if reliable record boundaries cannot be established. |
| **G1.1** | Bronze -> Silver | Mandatory identifiers and dates parse across the known date formats; UTC is converted according to the approved business-date rule. | **QUARANTINE** invalid records; **FAIL** if a required feed cannot be interpreted. |
| **G1.2** | Bronze -> Silver | Money/quantity values safely convert; cents-versus-dollars anomalies, percent-versus-amount NCR discounts, returns and void status are identified. | **QUARANTINE** ambiguous conversions or irreconcilable monetary rows; **ALERT** on elevated exception counts. |
| **G2.1** | Silver -> Gold | Source-system-qualified transaction + business date + line number is unique, and item/location identifiers map to canonical business keys. | **QUARANTINE** duplicate or unmapped fact rows; **FAIL** if required dimensions cannot be loaded. |
| **G2.2** | Silver -> Gold | Approved fiscal calendar has exactly one row per date, no required dates missing, and correct 4-5-4 week assignments. | **FAIL** calendar-dependent publication until corrected seed rows are approved. |
| **G2.3** | Silver -> Gold | Proposed `DIM_ITEM` / `DIM_LOCN` SCD2 versions have no overlaps and at most one current row per business key; fact FKs resolve as of the fact date. | **FAIL** the dimensional merge or dependent fact batch; no silent arbitrary FK. |
| **G2.4** | Silver -> Gold | Price event ranges and zone assignments resolve to one approved price per item/location/day; backwards intervals are invalid. | **QUARANTINE** unresolved overlaps and bad intervals; **FAIL** affected price publication if ambiguity remains. |
| **G2.5** | Silver -> Gold | Reconcile movement-based inventory to the next authoritative full snapshot; check negative/extreme positions and pack conversions. | **ALERT** discrepancies; **QUARANTINE** invalid conversions. Never silently force deltas to equal snapshots. |
| **G2.6** | Silver -> Gold | Each fact matches its 2.1 grain; mandatory FKs and measures follow defined nullability, unit and sign rules. | **FAIL** duplicate-grain publication; **QUARANTINE** offending fact rows. |
| **G3.1** | Gold mart -> consumers | Aggregate quantity and amounts equal sums of eligible `FCT_SALES` records by item/location/day; distinct transaction and line counts are checked. | **FAIL** mart publication on mismatch and **ALERT** the owner. |

A closure flag alone is not a reason to delete sales: store 1118 has activity after its recorded closure date. Flag the event for review and keep its source lineage. Numeric outliers should be assessed by business rules rather than blindly rejected: zeros, negatives and extreme values can be legitimate in sales, returns and stock feeds.

## 4. Transformations and grain rules

- **Sales:** Preserve the origin (`SRC_SYS_CD`) and use a durable **source-qualified** ticket key (source, business location, transaction date and ticket identifier), plus line number. A ticket number alone is not unique. Normalize negative returns and void handling, while keeping source evidence. Tax-inclusive net amounts and inconsistent discounts must be resolved or quarantined rather than forced into `gross - discount` without a rule.
- **Dimensions:** `DIM_DEPT`, `DIM_CATG`, `DIM_SUBCATG`, `DIM_REGN`, `DIM_CITY`, `DIM_CUSTOMER` use the SCD1 fields from the ERD. `DIM_ITEM` and `DIM_LOCN` maintain SCD2 history for their specified attributes. SCD1 corrections update a value; SCD2 corrections close the previous interval and add a new surrogate-key version. As-of joins preserve historical attribution.
- **Inventory:** `FCT_INVENTORY` has **one item-location effective-dated position**, with `EFF_FROM_DAY_KY` and `EFF_TO_DAY_KY`. A full SOH snapshot anchors a position; movement records may explain intermediate changes but are not guaranteed to reconcile. Changes after a source closure date are flagged. `ON_ORDER_QTY` and `RSRVD_QTY` must remain null/unknown unless a reliable source supplies them; do not manufacture zeroes.
- **Price:** `FCT_PRICE` has one effective price for each item-location-day. Proposed conflict ordering is *clearance, then promo, then regular*, then latest approved effective event; this **needs business sign-off**. An unresolved two-zone conflict is not silently resolved by an arbitrary join.
- **Aggregate:** `AGG_SALES_ITEM_LOCN_DAY` is derived only from published Gold sales. For `TXN_CNT`, count distinct durable transactions (not distinct bare ticket numbers); for `LINE_CNT`, count accepted transaction lines. Recompute the impacted grain on reload.

## 5. Partitioning, loads and retention

These are **proposed operational policies** for a future implementation, not properties verified in the provided source files.

| Stage | Logical partition / access unit | Load method | Proposed retention |
|---|---|---|---|
| Bronze | `source_system`, source file, ingest batch and source period; raw path preserved | **Full** for reference extracts and SOH snapshots, **delta** for transactions/movements, **seed** for fiscal calendar | **36 months**, original files immutable |
| Silver | Business date and source/batch; effective-date ranges for reference/price data | Rebuild affected staging partitions from Bronze; deduplicate by stable source-row identity | **24 months** of accepted staging; quarantines **180 days** after resolution, subject to audit policy |
| Gold dimensions | Business key / SCD2 effective dates (not necessarily physically partitioned) | SCD1 upsert; SCD2 effective-dated merge; `DIM_DAY` seed/approved correction | **7 years** of change history (proposed) |
| Gold facts | `DAY_KY` for sales and prices; effective-from day/range for inventory | Grain-key merge or atomic replacement of impacted business-date/range | **7 years** (proposed) |
| Gold aggregate | `DAY_KY` | Recompute/replace affected item-location-day groups from published facts | **7 years**, or rebuildable from retained Gold detail |

Use `BATCH_ID`, `SRC_SYS_CD` where present, and the ERD's insert/update timestamps for lineage. `DIM_DAY` does not include these audit attributes in 2.1, so its seed/version provenance should be kept in ingestion metadata rather than silently changing the model.

## 6. Reload policy: correct one bad day

1. **Identify** the affected business date, files, source systems and downstream facts using the ingestion manifest and exception log. For OMS, derive the business date from UTC using the approved location/channel timezone rule; do not equate UTC date with store date automatically.
2. **Preserve** the original Bronze files. Land any corrected extract as a new file/version with a new checksum and processing batch; do not overwrite the original. If the same file is replayed, use its identity to avoid doubling rows.
3. **Rebuild Silver** for that day from the relevant Bronze extracts. A day may live inside a **monthly** `TRAN_DATA` file, an **entire-season** POS/OMS file, or a **dated** SOH snapshot; select/filter the correct rows rather than assuming one file per day. Repeat all G1 and G2 checks and hold unresolved exceptions.
4. **Reconcile downstream history** before publishing: rebuild that day's sales facts using stable line identity, revise any impacted SCD2 effective ranges and as-of dimension keys, and recalculate impacted price-event days. For inventory, replay movements from the preceding full snapshot **through the next authoritative snapshot** (or the latest known boundary), flagging residual reconciliation differences. Do not rebuild just one inventory date if the position is effective across multiple days.
5. **Publish atomically** after passing gates. Replace only the impacted Gold grain/date or effective interval within a transaction or staging-to-publish swap; never append duplicate facts. Recompute the affected `AGG_SALES_ITEM_LOCN_DAY` groups and pass G3 before exposing them to reports.
6. **Record** the old and new batch IDs, checksums, exceptions, row counts, financial totals and publication status. On failure, keep the previous approved Gold version visible and the new run unpublished.

**Idempotency expectation:** Running the same input and correction rules twice should not change published Gold row counts or measures. A changed source extract is a new version, not a second copy of the business event.

## 7. Assumptions and open approvals

- Business owners must approve the chosen RMS-to-2.1 hierarchy projection, source priority rules for overlapping sales and price feeds, the price-event precedence, and acceptable inventory reconciliation variance.
- An authoritative city crosswalk and customer-identity availability have **not** been verified by the supplied catalog. Missing values are not invented; unidentified shoppers use an explicitly governed unknown customer where the fact requires a key.
- The source catalog reports bad calendar seed records, incompatible encodings, mixed currencies, repeated headers, source-specific return logic and inconsistent POS checks. The gates intentionally identify these issues rather than assume the source is already clean.
- The 2.1 ERD does not specify a forecast fact, so RPAS remains a retained source feed outside the modeled Gold outputs.
- Retention durations and source-specific thresholds in this document are design proposals; they require approval before implementation.
