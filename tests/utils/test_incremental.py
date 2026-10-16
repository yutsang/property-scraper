from datetime import date

import pandas as pd

from property_scraper.utils.incremental import merge_snapshot, plan_refresh


def test_plan_refresh_revisits_configured_lookback_window() -> None:
    plan = plan_refresh(
        latest_local_date=date(2026, 7, 22),
        today=date(2026, 7, 28),
        lookback_days=7,
    )

    assert plan.should_fetch is True
    assert plan.start_date == date(2026, 7, 16)


def test_merge_snapshot_replaces_rows_with_the_same_identity() -> None:
    existing = pd.DataFrame(
        [{"transaction_id": "T1", "price": 100, "status": "old"}]
    )
    fetched = pd.DataFrame(
        [{"transaction_id": "T1", "price": 100, "status": "corrected"}]
    )

    result = merge_snapshot(existing, fetched, ["transaction_id"])

    assert result.to_dict("records") == [
        {"transaction_id": "T1", "price": 100, "status": "corrected"}
    ]
