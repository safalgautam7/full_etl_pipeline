"""Add reproducible synthetic history without modifying existing CSVs or ETL.

Default range ends immediately before the existing November 2026 fixture.
Only the standard library is required. No warehouse connection is made.
"""

import argparse
import csv
import hashlib
import io
import json
import random
import re
from collections import Counter, defaultdict
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PATTERN = re.compile(r"(item_master|store_master|pos_sales|inventory_snapshot)_(\d{8})\.csv")
ANCHOR = date(2024, 11, 9)
VERSION = "synthetic-history-v1"


def digest(contents):
    return hashlib.sha256(contents).hexdigest()


def atomic_json(path, value):
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(value, indent=2, sort_keys=False) + "\n")
    temporary.replace(path)


def read_csv(path):
    with path.open(newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        return reader.fieldnames, list(reader)


def encode_csv(headers, rows):
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=headers, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue().encode("utf-8")


def build_day(day, seed, templates):
    """Return CSV bytes and explicit intended outcomes, not SQL classifications."""
    rng = random.Random(f"{VERSION}:{seed}:{day.isoformat()}")
    offset = (day - ANCHOR).days
    stamp = day.strftime("%Y%m%d")
    files = {}
    items = templates["item_master"][1]
    stores = templates["store_master"][1]

    def emit(source, rows, defects):
        rejected = [{"row": n, "reason": reason} for n, reason in defects]
        entry = {
            "rows": len(rows), "accepted": len(rows) - len(defects),
            "rejected": len(defects), "reasons": dict(Counter(r for _, r in defects)),
            "rejected_rows": rejected,
        }
        if source in ("pos_sales", "inventory_snapshot"):
            column = "net_sales" if source == "pos_sales" else "inventory_value"
            excluded = {n for n, _ in defects}
            raw = sum((Decimal(r[column]) for r in rows), Decimal("0.00"))
            accepted = sum((Decimal(r[column]) for n, r in enumerate(rows, 1)
                            if n not in excluded), Decimal("0.00"))
            entry.update(amount_column=column, raw_amount=f"{raw:.2f}",
                         accepted_amount=f"{accepted:.2f}", rejected_amount=f"{raw - accepted:.2f}")
        files[f"{source}_{stamp}.csv"] = (
            encode_csv(templates[source][0], rows), entry)

    # Full snapshots retain all original natural keys; invalid extras cannot
    # invalidate the clean master population used by sales and inventory.
    rows = [dict(r, snapshot_date=day.isoformat()) for r in items]
    rows.append(dict(rows[0], item_id=f"BADITEM{stamp}", list_price="-1.00"))
    emit("item_master", rows, [(len(rows), "INVALID_LIST_PRICE")])

    # Fixed weekly cadence independent of requested batch boundaries.
    if offset % 7 == 0:
        rows = [dict(r, snapshot_date=day.isoformat()) for r in stores]
        rows.append(dict(rows[0], store_id=f"BADSTORE{stamp}", is_active="?"))
        emit("store_master", rows, [(len(rows), "INVALID_IS_ACTIVE")])

    season = (100, 95, 100, 105, 110, 115, 110, 105, 110, 115, 125, 145)[day.month - 1]
    growth = 100 + (day.year - 2024) * 8
    weekend = 125 if day.isoweekday() >= 6 else 100
    count = max(30, 90 * season * growth * weekend // 1000000 + rng.randint(-8, 8))
    sales = []
    sold = Counter()
    for n in range(count):
        i = n % len(items)
        item, store = items[i], stores[i % len(stores)]
        quantity = rng.randint(1, 5)
        price = Decimal(item["list_price"])
        discount = Decimal("0.20") if n % 9 == 0 else Decimal("0.00")
        sales.append(dict(sale_date=day.isoformat(), transaction_id=f"HIST{stamp}-{n:05d}",
                          line_number="1", store_id=store["store_id"], item_id=item["item_id"],
                          quantity=str(quantity), unit_price=f"{price:.2f}",
                          discount_amount=f"{discount:.2f}",
                          net_sales=f"{quantity * price - discount:.2f}",
                          payment_method=rng.choice(["CASH", "CARD", "MOBILE"])))
        sold[i] += quantity
    defects = []
    changes = [
        ("ZERO_QUANTITY", {"quantity": "0", "discount_amount": "0.00", "net_sales": "0.00"}),
        ("NEGATIVE_QUANTITY", {"quantity": "-1"}),
        ("AMOUNT_MISMATCH", {"net_sales": "999.99"}),
        ("UNKNOWN_ITEM_ID", {"item_id": "HISTORY_UNKNOWN_ITEM"}),
        ("UNKNOWN_STORE_ID", {"store_id": "HISTORY_UNKNOWN_STORE"}),
    ]
    # Rotate isolated defects; append a duplicate of the first valid line.
    reason, updates = changes[offset % len(changes)]
    sales.append(dict(sales[0], transaction_id=f"HIST{stamp}-DEFECT", **updates))
    defects.append((len(sales), reason))
    sales.append(dict(sales[0]))
    defects.append((len(sales), "DUPLICATE_SALES_LINE"))
    emit("pos_sales", sales, defects)

    # Daily deterministic opening positions plus modeled replenishment. These
    # are synthetic positions, not a claim of full stock-flow accounting.
    inventory = []
    for i, original in enumerate(templates["inventory_snapshot"][1]):
        quantity = max(0, 70 + i * 2 - (offset % 7) * 5 - sold[i])
        cost = Decimal(original["unit_cost"])
        inventory.append(dict(original, snapshot_date=day.isoformat(),
                              quantity_on_hand=str(quantity), inventory_value=f"{quantity * cost:.2f}"))
    if offset % 2:
        inventory.append(dict(inventory[0], quantity_on_hand="-1"))
        reason = "INVALID_QUANTITY_ON_HAND"
    else:
        inventory.append(dict(inventory[0], inventory_value="9999.99"))
        reason = "INVENTORY_VALUE_MISMATCH"
    defects = [(len(inventory), reason)]
    inventory.append(dict(inventory[0]))
    defects.append((len(inventory), "DUPLICATE_INVENTORY_POSITION"))
    emit("inventory_snapshot", inventory, defects)
    return files


def generate(data, evidence, start, end, seed):
    if start < ANCHOR or end >= date(2026, 11, 9) or start > end:
        raise ValueError("Range must lie within 2024-11-09 through 2026-11-08")
    # Load every template from the immutable first fixture date.
    templates = {source: read_csv(data / f"{source}_20261109.csv") for source in
                 ("item_master", "store_master", "pos_sales", "inventory_snapshot")}
    manifest_path = data / "file_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    for entry in manifest["files"]:
        path = data / entry["file"]
        if digest(path.read_bytes()) != entry["sha256"]:
            raise ValueError(f"Existing manifest checksum mismatch: {path}")
        if len(read_csv(path)[1]) != entry["rows"]:
            raise ValueError(f"Existing manifest row-count mismatch: {path}")
    evidence.mkdir(parents=True, exist_ok=True)
    ledger_path = evidence / "synthetic_history_expected.json"
    ledger = json.loads(ledger_path.read_text()) if ledger_path.exists() else {
        "version": VERSION, "seed": seed, "scope": "Generated historical files only; excludes existing fixtures",
        "files": {},
    }
    if ledger["seed"] != seed or ledger["version"] != VERSION:
        raise ValueError("Existing ledger uses a different seed or generator version")
    pending = {}
    day = start
    while day <= end:
        pending.update(build_day(day, seed, templates))
        day += timedelta(days=1)
    # Preflight all paths before creating any CSV. Existing bytes must match.
    for name, (contents, _) in pending.items():
        path = data / name
        if path.exists() and path.read_bytes() != contents:
            raise ValueError(f"Refusing to overwrite different existing file: {path}")
    for name, (contents, entry) in pending.items():
        path = data / name
        if not path.exists():
            with path.open("xb") as stream:
                stream.write(contents)
        ledger["files"][name] = dict(entry, sha256=digest(contents))
    # Revalidate ledger files on resumption, including previous monthly batches.
    quarters = defaultdict(lambda: {"accepted_sales_rows": 0, "accepted_sales_cents": 0})
    for name, entry in sorted(ledger["files"].items()):
        if digest((data / name).read_bytes()) != entry["sha256"]:
            raise ValueError(f"Ledger checksum mismatch: {name}")
        if name.startswith("pos_sales_"):
            business_day = date.fromisoformat(f"{name[-12:-8]}-{name[-8:-6]}-{name[-6:-4]}")
            key = f"{business_day.year}-Q{(business_day.month - 1) // 3 + 1}"
            quarters[key]["accepted_sales_rows"] += entry["accepted"]
            quarters[key]["accepted_sales_cents"] += int(Decimal(entry["accepted_amount"]) * 100)
    ledger["files"] = dict(sorted(ledger["files"].items()))
    ledger["calendar_quarters"] = dict(sorted(quarters.items()))
    ledger["totals"] = {key: sum(e[key] for e in ledger["files"].values())
                        for key in ("rows", "accepted", "rejected")}
    entries = []
    for path in sorted(data.glob("*.csv")):
        match = PATTERN.fullmatch(path.name)
        if not match:
            continue
        business_day = date.fromisoformat(match[2])
        entries.append(dict(file=path.name, business_date=business_day.isoformat(),
                            rows=len(read_csv(path)[1]), sha256=digest(path.read_bytes())))
    manifest.update(version=f"{VERSION}-plus-original-fixtures",
                    files=entries, file_count=len(entries), total_rows=sum(e["rows"] for e in entries),
                    store_snapshot_dates=[e["business_date"] for e in entries
                                          if e["file"].startswith("store_master_")])
    atomic_json(ledger_path, ledger)
    atomic_json(manifest_path, manifest)
    return {"generated_files_in_range": len(pending), "manifest_files": len(entries),
            "generated_history_totals": ledger["totals"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=date.fromisoformat, default=ANCHOR)
    parser.add_argument("--end", type=date.fromisoformat, default=date(2026, 11, 8))
    parser.add_argument("--seed", type=int, default=20261109)
    args = parser.parse_args()
    print(json.dumps(generate(ROOT / "data", ROOT / "evidence", args.start, args.end, args.seed), indent=2))
