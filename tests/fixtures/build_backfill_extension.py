"""Deterministic fixture builder; never overwrites a changed/existing CSV.

Run from the repository root with Python. Expectations describe intentional
outcomes, not a second implementation of the production classifiers.
"""

import csv
import hashlib
import io
import json
from collections import Counter
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def build():
    files = {}
    ledger = {}
    for day in range(14, 19):
        date = f"2026-11-{day}"
        for source in ("item_master", "store_master", "pos_sales", "inventory_snapshot"):
            if source == "store_master" and day != 15:
                continue
            previous = 11 if source == "store_master" else 13
            with (ROOT / f"data/{source}_202611{previous}.csv").open(newline="") as stream:
                reader = csv.DictReader(stream)
                headers = reader.fieldnames
                baseline = list(reader)
            rows, cases = [], []

            def add(row, case, reason=None):
                rows.append(dict(row))
                cases.append({"row": len(rows), "case": case, "reason": reason})

            date_key = "sale_date" if source == "pos_sales" else "snapshot_date"
            for i, original in enumerate(baseline, 1):
                row = dict(original)
                row[date_key] = date
                if source == "item_master":
                    if row["item_id"] == "ITEM005" and day in (15, 16):
                        row["list_price"] = "5.25"
                    if row["item_id"] == "ITEM006" and day in (16, 17):
                        row["is_active"] = "N"
                    if row["item_id"] == "ITEM007" and day >= 17:
                        row.update(subclass_id="SC03", subclass_name="Home Care")
                    if row["subclass_id"] == "SC03" and day >= 16:
                        row["subclass_name"] = "Household Care"
                    if row["item_id"] == "ITEM008":
                        row["item_name"] = 'Cheddar Cheese, "Family" 200g'
                    if row["item_id"] == "ITEM030":
                        row.update(brand="", unit_of_measure=" ea ", is_active=" y ")
                elif source == "store_master":
                    if row["district_id"] == "DIST04":
                        row["district_name"] = "West District Updated"
                    if row["store_id"] == "STORE007":
                        row.update(city="Riverside", region="REGION_B")
                    if row["store_id"] == "STORE008":
                        row["is_active"] = "N"
                    if row["store_id"] == "STORE030":
                        row.update(city="", region="", is_active=" y ")
                elif source == "pos_sales":
                    # The original last-day input contains rejected rows. Build
                    # a clean baseline, then add explicit defects below.
                    row.update(transaction_id=f"EDGE{day}-{i:03d}", line_number="1",
                               store_id=f"STORE{i:03d}", item_id=f"ITEM{i:03d}",
                               quantity="2", unit_price="3.00", discount_amount="0.00",
                               net_sales="6.00", payment_method=" card ")
                else:
                    # Hold observations stable across days except an A-B-A
                    # quantity change and a stockout. Dimension-only changes
                    # still force new positional versions where applicable.
                    if i == 2 and day in (16, 17):
                        row["quantity_on_hand"] = str(int(row["quantity_on_hand"]) + 5)
                    if i == 3 and day >= 17:
                        row["quantity_on_hand"] = "0"
                    row["inventory_value"] = f"{Decimal(row['quantity_on_hand']) * Decimal(row['unit_cost']):.2f}"
                add(row, "baseline_history_and_normalization")

            seed = dict(rows[0])

            def defect(case, reason, **changes):
                row = dict(seed)
                if source == "pos_sales":
                    row["transaction_id"] = f"EDGE{day}-CASE-{len(rows) + 1}"
                row.update(changes)
                add(row, case, reason)

            defect("unparseable_business_date", "INVALID_SALE_DATE" if source == "pos_sales" else "INVALID_SNAPSHOT_DATE", **{date_key: "not-a-date"})
            defect("mismatched_business_date", "INVALID_SALE_DATE" if source == "pos_sales" else "INVALID_SNAPSHOT_DATE", **{date_key: "2026-11-19"})
            defect("blank_business_date", "INVALID_SALE_DATE" if source == "pos_sales" else "INVALID_SNAPSHOT_DATE", **{date_key: ""})
            if source in ("item_master", "store_master"):
                prefix = "item" if source == "item_master" else "store"
                group = "subclass" if source == "item_master" else "district"
                for field in (f"{prefix}_id", f"{prefix}_name", f"{group}_id", f"{group}_name"):
                    defect(f"blank_{field}", f"NULL_{field.upper()}", **{field: "  "})
                defect("invalid_active", "INVALID_IS_ACTIVE", is_active="TRUE")
                add(seed, "duplicate_master", f"DUPLICATE_{prefix.upper()}_SNAPSHOT")
                if source == "item_master":
                    for price in ("", "abc", "-0.01"):
                        defect(f"invalid_price_{price}", "INVALID_LIST_PRICE", list_price=price)
                    if day >= 16:
                        defect("new_item", None, item_id="ITEM031", item_name="New Seasonal Item", list_price="0.00")
                    defect("rejected_master_not_a_reference", "INVALID_LIST_PRICE", item_id="ITEM032", list_price="bad")
                else:
                    defect("new_store", None, store_id="STORE031", store_name="New Riverside Store")
                    defect("rejected_master_not_a_reference", "INVALID_IS_ACTIVE", store_id="STORE032", is_active="bad")
            else:
                for field in (("transaction_id", "store_id", "item_id") if source == "pos_sales" else ("store_id", "item_id")):
                    defect(f"blank_{field}", f"NULL_{field.upper()}", **{field: ""})
                if source == "pos_sales":
                    for value in ("", "0", "-1", "1.5", "abc"):
                        defect(f"invalid_line_{value}", "INVALID_LINE_NUMBER", line_number=value)
                    for value in ("", "abc", "1.5"):
                        defect(f"invalid_quantity_{value}", "INVALID_QUANTITY", quantity=value)
                    defect("negative_quantity", "NEGATIVE_QUANTITY", quantity="-1")
                    defect("zero_quantity", "ZERO_QUANTITY", quantity="0")
                    for field, reason in (("unit_price", "INVALID_UNIT_PRICE"), ("discount_amount", "INVALID_DISCOUNT")):
                        for value in ("", "bad", "-0.01"):
                            defect(f"invalid_{field}_{value}", reason, **{field: value})
                    defect("amount_outside_tolerance", "AMOUNT_MISMATCH", net_sales="6.02")
                    defect("amount_at_tolerance", None, net_sales="6.01")
                    defect("free_sale", None, unit_price="0.00", net_sales="0.00", payment_method="")
                    defect("full_discount", None, discount_amount="6.00", net_sales="0.00")
                    defect("existing_rule_allows_negative_net", None, discount_amount="7.00", net_sales="-1.00")
                    add(seed, "same_day_duplicate", "DUPLICATE_SALES_LINE")
                    if day > 14:
                        defect("cross_day_duplicate", "DUPLICATE_SALES_LINE", transaction_id="EDGE14-001")
                    defect("multiple_defects_primary_reason", "NULL_TRANSACTION_ID", transaction_id="", quantity="-1", item_id="")
                else:
                    for field, reason in (("quantity_on_hand", "INVALID_QUANTITY_ON_HAND"), ("unit_cost", "INVALID_UNIT_COST"), ("inventory_value", "INVALID_INVENTORY_VALUE")):
                        for value in ("", "bad", "-1"):
                            defect(f"invalid_{field}_{value}", reason, **{field: value})
                    defect("fractional_quantity", "INVALID_QUANTITY_ON_HAND", quantity_on_hand="1.5")
                    defect("value_outside_tolerance", "INVENTORY_VALUE_MISMATCH", inventory_value="60.82")
                    add(seed, "same_day_duplicate", "DUPLICATE_INVENTORY_POSITION")
                    # Unique positions for accepted boundary tests.
                    defect("zero_quantity_and_cost", None, store_id="STORE002", quantity_on_hand="0", unit_cost="0.00", inventory_value="0.00")
                    defect("value_at_tolerance", None, store_id="STORE003", inventory_value="60.81")
                defect("unknown_item", "UNKNOWN_ITEM_ID", item_id="ITEM999")
                defect("unknown_store", "UNKNOWN_STORE_ID", store_id="STORE999")
                defect("rejected_item_reference", "UNKNOWN_ITEM_ID", item_id="ITEM032")
                defect("future_then_current_item", "UNKNOWN_ITEM_ID" if day < 16 else None, item_id="ITEM031")
                defect("future_then_current_store", "UNKNOWN_STORE_ID" if day < 15 else None, store_id="STORE031")
                if day >= 15:
                    defect("rejected_store_reference", "UNKNOWN_STORE_ID", store_id="STORE032")
            filename = f"{source}_202611{day}.csv"
            stream = io.StringIO(newline="")
            writer = csv.DictWriter(stream, fieldnames=headers, lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
            files[filename] = stream.getvalue()
            ledger[filename] = {"rows": len(rows), "accepted": sum(c["reason"] is None for c in cases),
                                "reasons": dict(Counter(c["reason"] for c in cases if c["reason"])), "cases": cases}
            if source == "pos_sales":
                for label, subset in (("raw_amount", cases), ("accepted_amount", [c for c in cases if not c["reason"]]),
                                      ("rejected_amount", [c for c in cases if c["reason"]])):
                    ledger[filename][label] = str(sum((Decimal(rows[c["row"] - 1]["net_sales"]) for c in subset), Decimal("0.00")))
    return files, ledger


if __name__ == "__main__":
    files, ledger = build()
    # Validate every destination before writing any of them.
    for name, contents in files.items():
        path = ROOT / "data" / name
        if path.exists() and path.read_bytes() != contents.encode():
            raise SystemExit(f"Refusing to overwrite changed fixture: {path}")
    for name, contents in files.items():
        (ROOT / "data" / name).write_bytes(contents.encode())
    (ROOT / "tests/fixtures/backfill_extension_expected.json").write_text(json.dumps(ledger, indent=2) + "\n")
    manifest = {"version": "33-file-backfill-edge-cases", "files": []}
    for path in sorted((ROOT / "data").glob("*.csv")):
        with path.open(newline="") as stream:
            count = sum(1 for _ in csv.DictReader(stream))
        suffix = path.stem[-8:]
        manifest["files"].append({"file": path.name, "business_date": f"{suffix[:4]}-{suffix[4:6]}-{suffix[6:]}",
                                  "rows": count, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    manifest.update(file_count=len(manifest["files"]), total_rows=sum(f["rows"] for f in manifest["files"]),
                    store_snapshot_dates=[f["business_date"] for f in manifest["files"] if f["file"].startswith("store_master_")])
    (ROOT / "data/file_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
