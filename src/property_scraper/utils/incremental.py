"""Pure planning and merge operations for incremental source snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd


@dataclass(frozen=True)
class RefreshPlan:
    should_fetch: bool
    start_date: date | None
    reason: str


def plan_refresh(
    latest_local_date: date | None,
    today: date,
    *,
    lookback_days: int = 1,
) -> RefreshPlan:
    """Plan a refresh while allowing corrected recent records to be revisited."""
    if latest_local_date is None:
        return RefreshPlan(True, None, "no_local_snapshot")
    lookback = max(1, int(lookback_days))
    start_date = latest_local_date - timedelta(days=lookback - 1)
    return RefreshPlan(True, min(start_date, today), "incremental_lookback")


def merge_snapshot(
    existing: pd.DataFrame,
    fetched: pd.DataFrame,
    identity_columns: list[str],
) -> pd.DataFrame:
    """Merge fetched rows into a snapshot using one explicit identity contract."""
    missing = [
        column
        for column in identity_columns
        if column not in existing.columns and column not in fetched.columns
    ]
    if missing:
        raise KeyError(f"Snapshot identity columns are missing: {missing}")
    if existing.empty:
        combined = fetched.copy()
    elif fetched.empty:
        combined = existing.copy()
    else:
        combined = pd.concat([existing, fetched], ignore_index=True)
    return combined.drop_duplicates(
        subset=identity_columns,
        keep="last",
    ).reset_index(drop=True)
