from datetime import date

import pandas as pd

from property_scraper.pipelines.source_a_res.nodes import (
    _deduplicate_transaction_rows,
    _incremental_boundary,
    _partition_transaction_page,
    enrich_estate_data,
)
from property_scraper.utils.source_a_utils import parse_date_from_string


def test_parse_date_from_string_prefers_day_first_for_ambiguous_dates() -> None:
    assert parse_date_from_string("07/12/2026") == date(2026, 12, 7)
    assert parse_date_from_string("2026-07-15T00:00:00") == date(2026, 7, 15)


def test_enrich_estate_data_preserves_mixed_transaction_date_formats(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    transactions = pd.DataFrame(
        [
            {"transaction_id": "iso", "date": "2026-07-15"},
            {"transaction_id": "dmy", "date": "15/07/2026"},
            {"transaction_id": "ambiguous_dmy", "date": "07/12/2025"},
        ]
    )

    result = enrich_estate_data(pd.DataFrame(), transactions, {})

    assert result.set_index("transaction_id")["date"].to_dict() == {
        "iso": "15/07/2026",
        "dmy": "15/07/2026",
        "ambiguous_dmy": "07/12/2025",
    }


def test_incremental_boundary_includes_configured_lookback_window() -> None:
    assert _incremental_boundary(date(2026, 7, 22), 60) == date(2026, 5, 24)


def test_partition_transaction_page_does_not_stop_at_first_old_row() -> None:
    records = [
        {"transaction_id": "new-1", "date": "2026-07-20"},
        {"transaction_id": "old", "date": "2026-05-01"},
        {"transaction_id": "new-2", "date": "2026-07-15"},
    ]

    accepted, page_is_old = _partition_transaction_page(
        records, date(2026, 7, 1)
    )

    assert [record["transaction_id"] for record in accepted] == [
        "new-1",
        "new-2",
    ]
    assert page_is_old is False


def test_partition_transaction_page_marks_only_complete_old_page_stale() -> None:
    records = [
        {"transaction_id": "old-1", "date": "2026-05-01"},
        {"transaction_id": "old-2", "date": "2026-06-30"},
    ]

    accepted, page_is_old = _partition_transaction_page(
        records, date(2026, 7, 1)
    )

    assert accepted == []
    assert page_is_old is True


def test_deduplicate_transaction_rows_keeps_distinct_blank_id_rows() -> None:
    frame = pd.DataFrame(
        [
            {
                "transaction_id": "",
                "date": "2026-07-15",
                "address": "A",
                "price": 1,
                "area": 10,
            },
            {
                "transaction_id": "",
                "date": "2026-07-15",
                "address": "B",
                "price": 2,
                "area": 20,
            },
            {
                "transaction_id": "tx-1",
                "date": "2026-07-15",
                "address": "C",
                "price": 3,
                "area": 30,
            },
            {
                "transaction_id": "tx-1",
                "date": "2026-07-15",
                "address": "C updated",
                "price": 3,
                "area": 30,
            },
        ]
    )

    result = _deduplicate_transaction_rows(frame)
    expected_rows = 3

    assert len(result) == expected_rows
    assert set(result["address"]) == {"A", "B", "C updated"}
