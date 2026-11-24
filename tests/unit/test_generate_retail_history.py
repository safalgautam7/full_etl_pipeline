"""Credential-free tests for the additive historical data generator."""

import importlib.util
import json
import shutil
import tempfile
import unittest
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("history", ROOT / "scripts/generate_retail_history.py")
history = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(history)


class HistoryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.data = Path(self.temporary.name) / "data"
        self.data.mkdir()
        self.evidence = Path(self.temporary.name) / "evidence"
        for source in ("item_master", "store_master", "pos_sales", "inventory_snapshot"):
            name = f"{source}_20261109.csv"
            shutil.copyfile(ROOT / "data" / name, self.data / name)
        entries = [{"file": p.name, "rows": len(history.read_csv(p)[1]),
                    "sha256": history.digest(p.read_bytes())} for p in sorted(self.data.glob("*.csv"))]
        (self.data / "file_manifest.json").write_text(json.dumps({"version": "test", "files": entries}))

    def generate(self, start, end):
        return history.generate(self.data, self.evidence, start, end, 20261109)

    def test_batches_equal_single_run_and_are_idempotent(self):
        start = date(2024, 11, 9)
        end = date(2024, 11, 17)
        self.generate(start, start + timedelta(days=3))
        self.generate(start + timedelta(days=4), end)
        before = {p.name: p.read_bytes() for p in self.data.iterdir()}
        ledger_before = (self.evidence / "synthetic_history_expected.json").read_bytes()
        self.generate(start, end)
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.data.iterdir()})
        self.assertEqual(ledger_before, (self.evidence / "synthetic_history_expected.json").read_bytes())

    def test_conflict_refuses_before_creating_files(self):
        bad = self.data / "pos_sales_20241110.csv"
        bad.write_text("user data")
        with self.assertRaisesRegex(ValueError, "Refusing to overwrite"):
            self.generate(date(2024, 11, 9), date(2024, 11, 10))
        self.assertFalse((self.data / "item_master_20241109.csv").exists())
        self.assertEqual(bad.read_text(), "user data")

    def test_accounting_references_and_duplicate_order(self):
        self.generate(date(2024, 11, 9), date(2024, 11, 18))
        ledger = json.loads((self.evidence / "synthetic_history_expected.json").read_text())
        for name, entry in ledger["files"].items():
            rows = history.read_csv(self.data / name)[1]
            self.assertEqual(len(rows), entry["rows"])
            self.assertEqual(entry["rows"], entry["accepted"] + entry["rejected"])
            self.assertEqual(entry["rejected"], sum(entry["reasons"].values()))
            if "raw_amount" in entry:
                self.assertEqual(Decimal(entry["raw_amount"]),
                                 Decimal(entry["accepted_amount"]) + Decimal(entry["rejected_amount"]))
            if name.startswith("pos_sales_"):
                self.assertEqual(rows[0], rows[-1])
                rejected = {r["row"] for r in entry["rejected_rows"]}
                keys = set()
                for n, row in enumerate(rows, 1):
                    if n in rejected:
                        continue
                    key = row["transaction_id"], row["line_number"]
                    self.assertNotIn(key, keys)
                    keys.add(key)
                    self.assertTrue(row["item_id"].startswith("ITEM"))
                    self.assertTrue(row["store_id"].startswith("STORE"))
                    self.assertEqual(Decimal(row["net_sales"]), int(row["quantity"]) *
                                     Decimal(row["unit_price"]) - Decimal(row["discount_amount"]))


if __name__ == "__main__":
    unittest.main()
