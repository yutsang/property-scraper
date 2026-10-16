import pandas as pd

from property_scraper.pipelines.building_master.contracts import (
    CANDIDATE_PAIR_COLUMNS,
    SOURCE_RECORD_COLUMNS,
)
from property_scraper.pipelines.building_master.fingerprints import (
    FINGERPRINT_TIER,
    combine_candidate_pairs,
    generate_residential_fingerprint_candidates,
    shared_fingerprint_pairs,
    transaction_fingerprints,
)

SHARED_MIN = 2
COVERAGE_MIN = 0.30


def _centaline_transactions(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _prints(pairs: list[tuple[str, str]]) -> pd.DataFrame:
    return pd.DataFrame(pairs, columns=["building", "fingerprint"])


def test_fingerprints_normalise_the_two_sources_floor_and_flat_notation() -> None:
    """Centaline writes "7/F" and "Flat D"; Midland writes "7" and "D"."""
    centaline = transaction_fingerprints(
        _centaline_transactions(
            [{
                "building_code": "C1",
                "price": 4_958_500.0,
                "Floor": "7/F",
                "Flat": "Flat D",
                "area": 209.0,
                "transaction_type": "SALE",
            }]
        ),
        building_column="building_code",
        price_column="price",
        floor_column="Floor",
        flat_column="Flat",
        area_column="area",
        kind_column="transaction_type",
        sale_values={"SALE"},
    )
    midland = transaction_fingerprints(
        pd.DataFrame(
            [{
                "building_id": "M1",
                "price": 4_958_500,
                "floor": "7",
                "flat": "D",
                "net_area": 209,
                "tx_type": "S",
            }]
        ),
        building_column="building_id",
        price_column="price",
        floor_column="floor",
        flat_column="flat",
        area_column="net_area",
        kind_column="tx_type",
        sale_values={"S"},
    )

    assert set(centaline["fingerprint"]) == set(midland["fingerprint"])


def test_lettings_key_on_area_because_midland_masks_the_floor() -> None:
    midland = transaction_fingerprints(
        pd.DataFrame(
            [{
                "building_id": "M1",
                "price": 20000,
                "floor": None,
                "flat": "A",
                "net_area": 500,
                "tx_type": "L",
            }]
        ),
        building_column="building_id",
        price_column="price",
        floor_column="floor",
        flat_column="flat",
        area_column="net_area",
        kind_column="tx_type",
        sale_values={"S"},
    )

    assert len(midland) == 1
    assert midland.iloc[0]["fingerprint"].startswith("L|")


def test_pairs_need_both_enough_shared_deals_and_enough_coverage() -> None:
    left = _prints([("C1", "a"), ("C1", "b"), ("C1", "c"), ("C1", "d")])
    right = _prints([("M1", "a"), ("M1", "b")])

    pairs = shared_fingerprint_pairs(
        left, right, shared_min=SHARED_MIN, coverage_min=COVERAGE_MIN
    )

    assert len(pairs) == 1
    assert pairs.iloc[0]["shared"] == SHARED_MIN
    # 2 of Midland's 2 prints matched, so coverage is 1.0 even though Centaline
    # only had half of its own set matched.
    assert pairs.iloc[0]["coverage"] == 1.0


def test_a_single_shared_deal_is_not_enough() -> None:
    left = _prints([("C1", "a"), ("C1", "b")])
    right = _prints([("M1", "a"), ("M1", "z")])

    pairs = shared_fingerprint_pairs(
        left, right, shared_min=SHARED_MIN, coverage_min=COVERAGE_MIN
    )

    assert pairs.empty


def test_buildings_sharing_nothing_produce_no_pair() -> None:
    left = _prints([("C1", "a"), ("C1", "b")])
    right = _prints([("M1", "y"), ("M1", "z")])

    assert shared_fingerprint_pairs(
        left, right, shared_min=SHARED_MIN, coverage_min=COVERAGE_MIN
    ).empty


def _record(system: str, native_id: str, name: str) -> dict[str, object]:
    row: dict[str, object] = {column: pd.NA for column in SOURCE_RECORD_COLUMNS}
    row.update(
        {
            "source_record_id": f"{system}::residential::building::{native_id}",
            "source_system": system,
            "source_domain": "residential",
            "source_dataset": "transaction_building_candidates",
            "entity_grain": "building",
            "native_id": native_id,
            "name_en": name,
            "normalized_name": name.upper().replace(" ", ""),
            "district": "Sha Tin",
        }
    )
    return row


def test_differently_named_buildings_match_on_shared_transactions() -> None:
    """The whole point: names cannot identify these, the deals can."""
    records = pd.DataFrame([_record("centaline", "C1", "Tower 1"), _record("midland", "M1", "Block 1")])
    deals = [
        {"price": 5_000_000, "floor": "7", "flat": "A", "area": 500},
        {"price": 6_100_000, "floor": "9", "flat": "B", "area": 610},
    ]
    centaline = pd.DataFrame(
        [
            {
                "building_code": "C1",
                "price": d["price"],
                "Floor": f"{d['floor']}/F",
                "Flat": f"Flat {d['flat']}",
                "area": d["area"],
                "transaction_type": "SALE",
            }
            for d in deals
        ]
    )
    midland = pd.DataFrame(
        [
            {
                "building_id": "M1",
                "price": d["price"],
                "floor": d["floor"],
                "flat": d["flat"],
                "net_area": d["area"],
                "tx_type": "S",
            }
            for d in deals
        ]
    )

    candidates = generate_residential_fingerprint_candidates(records, centaline, midland)

    assert list(candidates.columns) == CANDIDATE_PAIR_COLUMNS
    assert len(candidates) == 1
    assert candidates.loc[0, "match_tier"] == FINGERPRINT_TIER
    assert candidates.loc[0, "left_source_record_id"].startswith("centaline::")


def test_combine_keeps_the_name_derived_row_for_a_pair_found_both_ways() -> None:
    def pair(tier: str) -> dict[str, object]:
        row: dict[str, object] = {column: pd.NA for column in CANDIDATE_PAIR_COLUMNS}
        row.update(
            {
                "left_source_record_id": "centaline::1",
                "right_source_record_id": "midland::2",
                "match_tier": tier,
                "match_score": 100.0,
            }
        )
        return row

    combined = combine_candidate_pairs(
        pd.DataFrame([pair("exact_bilingual")]), pd.DataFrame([pair(FINGERPRINT_TIER)])
    )

    assert len(combined) == 1
    assert combined.loc[0, "match_tier"] == "exact_bilingual"
