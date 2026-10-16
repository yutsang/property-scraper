import pandas as pd
import pytest

from property_scraper.pipelines.source_b_res.nodes import (
    load_existing_transactions,
    merge_transactions_estates,
    normalize_all_columns,
)


def test_normalize_estates_collapses_identical_cross_district_records() -> None:
    duplicate = {
        "id": "E1",
        "name": "Example Estate",
        "region": {"id": "10", "name": "Hong Kong Island"},
    }

    result = normalize_all_columns(pd.DataFrame([duplicate, duplicate]))

    assert len(result) == 1
    assert result.loc[0, "id"] == "E1"


def test_normalize_estates_rejects_conflicting_duplicate_ids() -> None:
    estates = pd.DataFrame(
        [
            {"id": "E1", "name": "Example Estate"},
            {"id": "E1", "name": "Conflicting Estate"},
        ]
    )

    with pytest.raises(ValueError, match="Conflicting estate records"):
        normalize_all_columns(estates)


def test_estate_enrichment_preserves_transaction_cardinality() -> None:
    transactions = pd.DataFrame(
        [
            {"transaction_id": "T1", "estate_id": "E1"},
            {"transaction_id": "T2", "estate_id": "E1"},
        ]
    )
    estates = pd.DataFrame([{"id": "E1", "name": "Example Estate"}])

    result = merge_transactions_estates(transactions, estates)

    assert len(result) == len(transactions)
    assert result["transaction_id"].tolist() == ["T1", "T2"]


def test_estate_enrichment_rejects_duplicate_right_side_ids() -> None:
    transactions = pd.DataFrame([{"transaction_id": "T1", "estate_id": "E1"}])
    estates = pd.DataFrame(
        [
            {"id": "E1", "name": "Example Estate"},
            {"id": "E1", "name": "Example Estate"},
        ]
    )

    with pytest.raises(pd.errors.MergeError):
        merge_transactions_estates(transactions, estates)


def test_corrupt_transaction_snapshot_fails_in_loader(tmp_path) -> None:
    corrupt = tmp_path / "transactions.parquet"
    corrupt.write_text("not parquet")

    with pytest.raises(Exception, match="Parquet|magic bytes"):
        load_existing_transactions(
            {"source_b_res": {"res_trans_path": str(corrupt)}}
        )
