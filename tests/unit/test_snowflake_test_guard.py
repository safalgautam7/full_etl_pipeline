import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

PATH = Path(__file__).resolve().parents[1] / "integration/test_snowflake_silver.py"
SPEC = importlib.util.spec_from_file_location("snowflake_test_guard", PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class SnowflakeGuardTests(unittest.TestCase):
    def test_production_or_arbitrary_database_rejected_before_connection(self):
        for database in ("RETAIL_DWH", "OTHER", "retail_etl_test_lowercase", "RETAIL_ETL_TEST_X;DROP DATABASE X"):
            with self.subTest(database=database), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "connection.json"
                path.write_text(json.dumps({"database": database}))
                with patch.dict(os.environ, {"RETAIL_TEST_SNOWFLAKE_CONFIG": str(path)}), self.assertRaises(ValueError):
                    MODULE.guarded_settings()

    def test_explicit_test_database_allowed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "connection.json"
            path.write_text(json.dumps({"database": "RETAIL_ETL_TEST_SILVER"}))
            with patch.dict(os.environ, {"RETAIL_TEST_SNOWFLAKE_CONFIG": str(path)}):
                self.assertEqual("RETAIL_ETL_TEST_SILVER", MODULE.guarded_settings()["database"])


if __name__ == "__main__":
    unittest.main()
