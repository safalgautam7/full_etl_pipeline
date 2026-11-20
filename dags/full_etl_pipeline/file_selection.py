"""Credential-free inclusive file selection and master dependency resolution."""

import re
from datetime import datetime

FILE_PATTERN = re.compile(
    r"^(item_master|store_master|pos_sales|inventory_snapshot)_(\d{8})\.csv$"
)


def inspect_filename(filename):
    match = FILE_PATTERN.fullmatch(filename)
    if not match:
        raise ValueError(f"Unexpected CSV name: {filename}")
    source, date = match.groups()
    return source, datetime.strptime(date, "%Y%m%d").date()


def select_files(filenames, file_limit=0, start_date=None, end_date=None):
    """Return selected filenames and additional earlier master snapshots.

    Missing days or masters are not a readiness gate. No range preserves the
    legacy alphabetical selection; a range cannot be truncated by a limit.
    """
    limit = int(file_limit)
    if limit < 0:
        raise ValueError("file_limit must be 0 (all) or a positive number")
    if (start_date is None) != (end_date is None):
        raise ValueError("Provide both start_date and end_date, or neither")
    start = end = None
    if start_date is not None:
        bounds = []
        for label, value in (("start_date", start_date), ("end_date", end_date)):
            if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                raise ValueError(f"{label} must use YYYY-MM-DD")
            try:
                bounds.append(datetime.strptime(value, "%Y-%m-%d").date())
            except ValueError as exc:
                raise ValueError(f"{label} must be a valid YYYY-MM-DD date") from exc
        start, end = bounds
        if start > end:
            raise ValueError("start_date must be on or before end_date")
        if limit:
            raise ValueError("Use file_limit=0 with a date range")
    names = sorted(name for name in filenames if FILE_PATTERN.fullmatch(name))
    if not names:
        raise ValueError("No recognized CSV inputs")
    if start is None:
        return (names if limit == 0 else names[:limit]), []
    dated = [(name, *inspect_filename(name)) for name in names]
    selected = {name for name, _, date in dated if start <= date <= end}
    if not selected:
        raise ValueError(f"No recognized CSV inputs in range {start_date} to {end_date}")
    dependencies = set()
    for source in ("item_master", "store_master"):
        candidates = [(date, name) for name, kind, date in dated if kind == source and date <= start]
        if candidates:
            _, name = max(candidates)
            if name not in selected:
                dependencies.add(name)
    return sorted(selected | dependencies), sorted(dependencies)
