import csv
import hashlib
import importlib.util
import json
import shutil
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("enrich", ROOT / "scripts/enrich_reporting_inputs.py")
enrich = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(enrich)


class SalesReportingInputTests(unittest.TestCase):
    def test_mappings_are_explicitly_synthetic_and_hierarchy_is_consistent(self):
        mapping = json.loads((ROOT / "config/sales_reporting_mappings.json").read_text())
        self.assertIn("SYNTHETIC", mapping["purpose"])
        parents = {}
        for hierarchy in mapping["subclasses"].values():
            self.assertTrue(all(hierarchy[k] for k in enrich.ITEM_FIELDS))
            parent = (hierarchy["department_id"], hierarchy["department_name"])
            self.assertEqual(parent, parents.setdefault(hierarchy["class_id"], parent))
        self.assertGreaterEqual(len(set(parents.values())), 3)

    def test_enrichment_backs_up_and_preserves_every_original_business_value(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "data").mkdir()
            (root / "config").mkdir()
            (root / "evidence").mkdir()
            shutil.copy2(ROOT / "config/sales_reporting_mappings.json", root / "config")
            originals = {}
            # Strip only the new fields to exercise a genuinely old input.
            for source in ("item_master", "store_master", "pos_sales", "inventory_snapshot"):
                name = f"{source}_20261109.csv"
                with (ROOT / "data" / name).open(newline="") as stream:
                    reader = csv.DictReader(stream)
                    fields = [k for k in reader.fieldnames if k not in enrich.ITEM_FIELDS + enrich.STORE_FIELDS]
                    rows = [{k: r[k] for k in fields} for r in reader]
                with (root / "data" / name).open("w", newline="") as stream:
                    writer = csv.DictWriter(stream, fieldnames=fields)
                    writer.writeheader()
                    writer.writerows(rows)
                originals[name] = ((root / "data" / name).read_bytes(), rows)
            entries = [{"file": n, "rows": len(rows), "sha256": hashlib.sha256(content).hexdigest()}
                       for n, (content, rows) in originals.items()]
            (root / "data/file_manifest.json").write_text(json.dumps({"files": entries}))
            self.assertEqual(2, enrich.migrate(root)["changed_master_files"])
            result = enrich.migrate(root, apply=True)
            backup = Path(result["backup"])
            for name, (content, old_rows) in originals.items():
                self.assertEqual(content, (backup / "data" / name).read_bytes())
                if name.startswith(("pos_sales", "inventory_snapshot")):
                    self.assertEqual(content, (root / "data" / name).read_bytes())
                with (root / "data" / name).open(newline="") as stream:
                    new_rows = list(csv.DictReader(stream))
                self.assertEqual(old_rows, [{k: r[k] for k in old_rows[0]} for r in new_rows])
            self.assertEqual(0, enrich.migrate(root, apply=True)["changed_master_files"])

    def test_existing_history_supports_q4_and_two_prior_quarters_with_iso_py(self):
        dates = {date.fromisoformat(f"{p.stem[-8:-4]}-{p.stem[-4:-2]}-{p.stem[-2:]}")
                 for p in (ROOT / "data").glob("pos_sales_*.csv")}
        day = date(2026, 4, 1)
        while day <= date(2026, 11, 18):
            self.assertIn(day, dates)
            iso = day.isocalendar()
            self.assertIn(date.fromisocalendar(iso.year - 1, iso.week, iso.weekday), dates)
            day += timedelta(days=1)
