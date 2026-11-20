"""Calendar loading without Airflow imports; explicit, replay-stable coverage."""

from datetime import date
from pathlib import Path


def calendar_bounds(cfg):
    start = date.fromisoformat(cfg.get("calendar_start", "2020-01-01"))
    end = date.fromisoformat(cfg.get("calendar_end", "2035-12-31"))
    if start > end or (end - start).days >= 73049:
        raise ValueError("Calendar must be ordered and cover at most 73049 days")
    if (start.month, start.day) != (1, 1) or (end.month, end.day) != (12, 31):
        raise ValueError("Calendar coverage must contain complete years")
    return start.isoformat(), end.isoformat()


def load_calendar(cursor, cfg):
    bounds = calendar_bounds(cfg)
    sql = (Path(cfg["gold_sql_dir"]) / "load_dim_date.sql").read_text().strip().rstrip(";")
    cursor.execute(sql, bounds)
