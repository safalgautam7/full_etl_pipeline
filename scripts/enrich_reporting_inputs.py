"""Enrich master CSVs for the sales star; never rewrite sales/inventory values.

Explicit --apply takes a complete input/evidence backup before replacing any
master files. This is a controlled fixture migration, not ordinary ingestion.
No Snowflake connection is made. Mappings are explicitly synthetic.
"""

import argparse
import csv
import hashlib
import io
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ITEM_FIELDS = ("department_id", "department_name", "class_id", "class_name")
STORE_FIELDS = ("location_type",)


def enrich_row(source, row, mappings):
    result = dict(row)
    if source == "item_master":
        hierarchy = mappings["subclasses"].get(row.get("subclass_id", "").strip())
        # Deliberately invalid original subclass records stay invalid under the
        # original priority rules; do not turn their missing subclass into real data.
        for key in ITEM_FIELDS:
            result[key] = hierarchy[key] if hierarchy else "UNMAPPED"
    elif source == "store_master":
        match = re.fullmatch(r"STORE(\d+)", row.get("store_id", "").strip())
        index = int(match[1]) - 1 if match else 0
        result["location_type"] = mappings["location_types"][index % len(mappings["location_types"])]
    else:
        raise ValueError("Only master sources can be enriched")
    return result


def enriched_bytes(path, mappings):
    source = path.name.rsplit("_", 1)[0]
    with path.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        headers = list(reader.fieldnames or [])
        extra = ITEM_FIELDS if source == "item_master" else STORE_FIELDS
        headers += [name for name in extra if name not in headers]
        rows = [enrich_row(source, row, mappings) for row in reader]
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=headers, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode("utf-8"), len(rows)


def migrate(root, apply=False):
    root = Path(root)
    data = root / "data"
    mappings = json.loads((root / "config/sales_reporting_mappings.json").read_text())
    pending = {}
    for pattern in ("item_master_*.csv", "store_master_*.csv"):
        for path in sorted(data.glob(pattern)):
            content, count = enriched_bytes(path, mappings)
            if content != path.read_bytes():
                pending[path] = (content, count)
    summary = {"changed_master_files": len(pending), "synthetic": True, "applied": False}
    if not apply or not pending:
        return summary
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = root / "data_backups" / f"before_sales_reporting_{stamp}"
    shutil.copytree(data, backup / "data")
    for name in ("synthetic_history_expected.json", "synthetic_history_README.md"):
        source = root / "evidence" / name
        if source.exists():
            shutil.copy2(source, backup / name)
    # All replacements were prepared before the first write. Originals stay in
    # the backup so an interrupted migration is recoverable; rerunning is safe.
    for path, (content, _) in pending.items():
        temporary = path.with_suffix(".csv.reporting.tmp")
        temporary.write_bytes(content)
        temporary.replace(path)
    manifest_path = data / "file_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    ledger_path = root / "evidence/synthetic_history_expected.json"
    ledger = json.loads(ledger_path.read_text()) if ledger_path.exists() else None
    for entry in manifest["files"]:
        content = (data / entry["file"]).read_bytes()
        entry["sha256"] = hashlib.sha256(content).hexdigest()
        if ledger and entry["file"] in ledger["files"]:
            ledger["files"][entry["file"]]["sha256"] = entry["sha256"]
    manifest["reporting_attributes"] = mappings["version"]
    manifest["synthetic"] = True
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    if ledger:
        ledger["reporting_attributes"] = mappings["version"]
        ledger_path.write_text(json.dumps(ledger, indent=2) + "\n")
    summary.update(applied=True, backup=str(backup))
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Back up inputs, then enrich master files")
    args = parser.parse_args()
    print(json.dumps(migrate(ROOT, args.apply), indent=2))
