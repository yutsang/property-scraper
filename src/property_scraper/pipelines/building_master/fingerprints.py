"""Match buildings by the transactions they share rather than by their names.

Names cannot identify a large part of the residential stock: Centaline holds no
Chinese name for any residential building, Midland holds no address for any, and
neither side has coordinates. What both sides do record is the transactions
themselves, so two records describing one building will report the same deals.

A fingerprint is one transaction reduced to the fields both sources publish.
Sales key on floor; lettings key on area instead, because Midland masks the
floor on every residential letting.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Mapping

import pandas as pd

from .contracts import CANDIDATE_PAIR_COLUMNS
from .matching import (
    _completion_agrees,
    _geography_agrees,
    _normal_address,
    _normal_name,
    _params,
    _property_type_agrees,
    _score,
)

FINGERPRINT_TIER = "transaction_fingerprint"
CROSS_GRAIN_TIER = "cross_grain_alias"

_DEFAULT_SHARED_MIN = 2
_DEFAULT_COVERAGE_MIN = 0.30


def _normalized_floor(series: pd.Series) -> pd.Series:
    """Centaline writes "7/F", Midland writes "7"."""
    text = series.astype(str).str.upper().str.strip()
    text = text.str.replace(r"\s*/\s*F\.?$", "", regex=True)
    return text.str.replace(r"[^A-Z0-9]", "", regex=True)


def _normalized_flat(series: pd.Series) -> pd.Series:
    """Centaline writes "Flat D", Midland writes "D"."""
    text = series.astype(str).str.upper().str.strip()
    text = text.str.replace(r"^(?:FLAT|UNIT|ROOM)\s*", "", regex=True)
    return text.str.replace(r"[^A-Z0-9]", "", regex=True)


def _rounded(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").round().astype("Int64").astype(str)


def transaction_fingerprints(
    transactions: pd.DataFrame,
    *,
    building_column: str,
    price_column: str,
    floor_column: str,
    flat_column: str,
    area_column: str,
    kind_column: str,
    sale_values: set[str],
) -> pd.DataFrame:
    """Reduce transactions to (building, fingerprint) pairs, dropping rows whose
    identifying fields are blank on either side of the comparison."""
    missing = [
        column
        for column in (
            building_column,
            price_column,
            floor_column,
            flat_column,
            area_column,
            kind_column,
        )
        if column not in transactions.columns
    ]
    if missing:
        raise KeyError(f"Transaction frame is missing columns: {missing}")

    price = _rounded(transactions[price_column])
    floor = _normalized_floor(transactions[floor_column])
    flat = _normalized_flat(transactions[flat_column])
    area = _rounded(transactions[area_column])
    is_sale = transactions[kind_column].astype(str).str.upper().isin(sale_values)

    fingerprint = ("S|" + price + "|" + floor + "|" + flat).where(
        is_sale, "L|" + price + "|" + flat + "|" + area
    )
    unusable = (
        price.eq("<NA>")
        | flat.eq("")
        | (is_sale & floor.eq(""))
        | (~is_sale & area.eq("<NA>"))
    )
    frame = pd.DataFrame(
        {
            "building": transactions[building_column].astype(str),
            "fingerprint": fingerprint,
        }
    )
    return frame.loc[~unusable & frame["building"].ne("nan")].drop_duplicates()


def shared_fingerprint_pairs(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    shared_min: int,
    coverage_min: float,
) -> pd.DataFrame:
    """Return cross-source building pairs that report the same transactions.

    Coverage is the larger of the two shares so a small building fully contained
    in a larger one's transaction set still qualifies; requiring both sides to
    agree would drop pairs where one source covers a building only partially.
    """
    left_sets = left.groupby("building")["fingerprint"].apply(set)
    right_sets = right.groupby("building")["fingerprint"].apply(set)
    if left_sets.empty or right_sets.empty:
        return pd.DataFrame(columns=["left", "right", "shared", "coverage"])

    index: defaultdict[str, list[str]] = defaultdict(list)
    for building, prints in right_sets.items():
        for print_ in prints:
            index[print_].append(building)

    shared: defaultdict[tuple[str, str], int] = defaultdict(int)
    for building, prints in left_sets.items():
        for print_ in prints:
            for other in index.get(print_, ()):
                shared[(building, other)] += 1

    rows = []
    for (left_id, right_id), count in shared.items():
        if count < shared_min:
            continue
        coverage = max(
            count / len(left_sets[left_id]), count / len(right_sets[right_id])
        )
        if coverage >= coverage_min:
            rows.append(
                {
                    "left": left_id,
                    "right": right_id,
                    "shared": count,
                    "coverage": round(coverage, 4),
                }
            )
    return pd.DataFrame(rows, columns=["left", "right", "shared", "coverage"])


def _candidate(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
    shared: int,
    coverage: float,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "left_source_record_id": left["source_record_id"],
        "right_source_record_id": right["source_record_id"],
        "source_domain": left["source_domain"],
        "entity_grain": left["entity_grain"],
        "match_tier": FINGERPRINT_TIER,
        "match_score": round(coverage * 100, 3),
        "name_score": _score(_normal_name(left), _normal_name(right)),
        "address_score": _score(_normal_address(left), _normal_address(right)),
        "geography_agrees": _geography_agrees(left, right, config),
        "completion_year_agrees": _completion_agrees(left, right, config),
        "property_type_agrees": _property_type_agrees(left, right),
        "is_ambiguous": False,
        "decision_reason": f"{shared} shared transaction fingerprints",
    }


def build_fingerprint_candidates(
    source_records: pd.DataFrame,
    left_prints: pd.DataFrame,
    right_prints: pd.DataFrame,
    *,
    source_domain: str,
    params: Mapping[str, Any] | None = None,
) -> pd.DataFrame:
    """Turn shared transaction evidence into contract-shaped candidate pairs."""
    config = _params(params)
    shared_min = int(config.get("fingerprint_shared_min", _DEFAULT_SHARED_MIN))
    coverage_min = float(
        config.get("fingerprint_coverage_min", _DEFAULT_COVERAGE_MIN)
    )
    pairs = shared_fingerprint_pairs(
        left_prints, right_prints, shared_min=shared_min, coverage_min=coverage_min
    )
    if pairs.empty:
        return pd.DataFrame(columns=CANDIDATE_PAIR_COLUMNS)

    scope = source_records[
        source_records["source_domain"].eq(source_domain)
        & source_records["entity_grain"].eq("building")
    ]
    by_native: dict[tuple[str, str], dict[str, Any]] = {}
    for record in scope.to_dict(orient="records"):
        native_id = record.get("native_id")
        if pd.isna(native_id):
            continue
        by_native[(str(record["source_system"]), str(native_id))] = record

    rows = []
    for pair in pairs.to_dict(orient="records"):
        left = by_native.get(("centaline", pair["left"]))
        right = by_native.get(("midland", pair["right"]))
        if left is None or right is None:
            continue
        rows.append(
            _candidate(left, right, int(pair["shared"]), float(pair["coverage"]), config)
        )
    if not rows:
        return pd.DataFrame(columns=CANDIDATE_PAIR_COLUMNS)
    frame = pd.DataFrame(rows)
    # Deterministic ordering: strongest evidence first, ids to break ties.
    return frame.sort_values(
        ["match_score", "left_source_record_id", "right_source_record_id"],
        ascending=[False, True, True],
        kind="stable",
    ).reindex(columns=CANDIDATE_PAIR_COLUMNS).reset_index(drop=True)


def commercial_fingerprints(
    transactions: pd.DataFrame,
    *,
    building_column: str,
    amount_columns: tuple[str, ...],
    area_column: str,
    floor_column: str,
    kind_column: str,
    sale_values: set[str],
) -> pd.DataFrame:
    """Commercial deals key on amount, area and floor band.

    The unit number is absent on 71% of Centaline's rows, and both sources record
    the floor only as HIGH/MID/LOW, so neither can stand in for the unit. Sale
    and letting amounts live in separate columns on the Centaline side.
    """
    missing = [
        column
        for column in (building_column, area_column, floor_column, kind_column)
        + tuple(amount_columns)
        if column not in transactions.columns
    ]
    if missing:
        raise KeyError(f"Transaction frame is missing columns: {missing}")

    amount = pd.Series(0.0, index=transactions.index)
    for column in amount_columns:
        values = pd.to_numeric(transactions[column], errors="coerce").fillna(0)
        amount = amount.where(amount > 0, values)
    kind = pd.Series("L", index=transactions.index)
    kind = kind.where(
        ~transactions[kind_column].astype(str).str.upper().isin(sale_values), "S"
    )
    area = _rounded(transactions[area_column])
    floor = transactions[floor_column].astype(str).str.upper().str.replace(
        r"[^A-Z0-9]", "", regex=True
    )
    frame = pd.DataFrame(
        {
            "building": transactions[building_column].astype(str),
            "fingerprint": kind
            + "|"
            + amount.round().astype("Int64").astype(str)
            + "|"
            + area
            + "|"
            + floor,
        }
    )
    unusable = amount.le(0) | area.isin(["<NA>", "0"]) | floor.eq("")
    unusable |= frame["building"].isin(["nan", "None", "<NA>", ""])
    return frame.loc[~unusable].drop_duplicates()


def generate_commercial_fingerprint_candidates(
    source_records: pd.DataFrame,
    centaline_transactions: pd.DataFrame,
    midland_transactions: pd.DataFrame,
    params: Mapping[str, Any] | None = None,
) -> pd.DataFrame:
    """Kedro node: commercial building pairs evidenced by shared transactions."""
    left = commercial_fingerprints(
        centaline_transactions,
        building_column="propertyId",
        amount_columns=("price", "rental"),
        area_column="transactionArea",
        floor_column="floor",
        kind_column="transactionType",
        sale_values={"S"},
    )
    right = commercial_fingerprints(
        midland_transactions,
        building_column="building_id",
        amount_columns=("price",),
        area_column="area",
        floor_column="floor",
        kind_column="tx_type",
        sale_values={"S"},
    )
    # Midland publishes no amount on 72% of ICI deals, so a commercial building's
    # transaction sets overlap far less than a residential one's. Both thresholds
    # are looser here to compensate.
    config = dict(_params(params))
    config["fingerprint_shared_min"] = config.get("fingerprint_commercial_shared_min", 2)
    config["fingerprint_coverage_min"] = config.get(
        "fingerprint_commercial_coverage_min", 0.10
    )
    return build_fingerprint_candidates(
        source_records,
        left,
        right,
        source_domain="commercial",
        params=config,
    )


def generate_cross_grain_candidates(
    source_records: pd.DataFrame, params: Mapping[str, Any] | None = None
) -> pd.DataFrame:
    """Pair records that describe one building at two different grains.

    Centaline lists 1005 Canton Road once as an estate_phase and again as a
    building. Matching only ever compares records of the same grain and of
    different sources, so these never meet and the building list carries both.

    Requiring the name *and* the address to match exactly keeps multi-block
    estates out: their buildings are named "Tower 1", not after the estate.
    """
    config = _params(params)
    scope = source_records[
        source_records["normalized_name"].fillna("").ne("")
        & source_records["normalized_address"].fillna("").ne("")
    ]
    rows: list[dict[str, Any]] = []
    for _, group in scope.groupby(
        ["normalized_name", "normalized_address"], dropna=False, sort=True
    ):
        if group["entity_grain"].nunique() < 2:
            continue
        records = sorted(
            group.to_dict(orient="records"),
            key=lambda record: str(record["source_record_id"]),
        )
        # Chain the group rather than pairing every combination: resolution
        # merges by connected component, so a chain is enough to unite them.
        for left, right in zip(records, records[1:]):
            if left["entity_grain"] == right["entity_grain"]:
                continue
            rows.append(
                {
                    "left_source_record_id": left["source_record_id"],
                    "right_source_record_id": right["source_record_id"],
                    "source_domain": left["source_domain"],
                    "entity_grain": left["entity_grain"],
                    "match_tier": CROSS_GRAIN_TIER,
                    "match_score": 100.0,
                    "name_score": 100.0,
                    "address_score": 100.0,
                    "geography_agrees": _geography_agrees(left, right, config),
                    "completion_year_agrees": _completion_agrees(left, right, config),
                    "property_type_agrees": _property_type_agrees(left, right),
                    "is_ambiguous": False,
                    "decision_reason": "same name and address at another grain",
                }
            )
    if not rows:
        return pd.DataFrame(columns=CANDIDATE_PAIR_COLUMNS)
    return pd.DataFrame(rows).reindex(columns=CANDIDATE_PAIR_COLUMNS)


def combine_candidate_pairs(*frames: pd.DataFrame) -> pd.DataFrame:
    """Merge the candidate sources in priority order, first occurrence winning.

    Name evidence is passed first so a pair found both ways keeps its
    name-derived row, which carries a resolved tier and the ambiguity flag the
    resolver depends on.
    """
    usable = [
        frame.reindex(columns=CANDIDATE_PAIR_COLUMNS)
        for frame in frames
        if frame is not None and not frame.empty
    ]
    if not usable:
        return pd.DataFrame(columns=CANDIDATE_PAIR_COLUMNS)
    return (
        pd.concat(usable, ignore_index=True)
        .drop_duplicates(
            subset=["left_source_record_id", "right_source_record_id"], keep="first"
        )
        .reset_index(drop=True)
    )


def generate_residential_fingerprint_candidates(
    source_records: pd.DataFrame,
    centaline_transactions: pd.DataFrame,
    midland_transactions: pd.DataFrame,
    params: Mapping[str, Any] | None = None,
) -> pd.DataFrame:
    """Kedro node: residential building pairs evidenced by shared transactions."""
    left = transaction_fingerprints(
        centaline_transactions,
        building_column="building_code",
        price_column="price",
        floor_column="Floor",
        flat_column="Flat",
        area_column="area",
        kind_column="transaction_type",
        sale_values={"SALE"},
    )
    right = transaction_fingerprints(
        midland_transactions,
        building_column="building_id",
        price_column="price",
        floor_column="floor",
        flat_column="flat",
        area_column="net_area",
        kind_column="tx_type",
        sale_values={"S"},
    )
    return build_fingerprint_candidates(
        source_records,
        left,
        right,
        source_domain="residential",
        params=params,
    )
