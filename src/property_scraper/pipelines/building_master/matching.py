"""Conservative, deterministic resolution for the consolidated building master.

The functions in this module deliberately operate only on data frames.  They
perform no I/O, making them suitable as Kedro nodes and straightforward to
exercise with small source-record fixtures.
"""

from __future__ import annotations

import hashlib
import math
import re
from collections import defaultdict
from collections.abc import Iterable, Mapping
from difflib import SequenceMatcher
from typing import Any

import pandas as pd

from ...utils.building_supplement import normalize_building_name
from ..data_process.workbook import sanitize_dataframe_content
from .contracts import (
    BRIDGE_COLUMNS,
    CANDIDATE_PAIR_COLUMNS,
    CANONICAL_COLUMNS,
    OVERRIDE_COLUMNS,
    REVIEW_COLUMNS,
    RULE_VERSION,
    SOURCE_RECORD_COLUMNS,
)

try:  # rapidfuzz is a project dependency, but retain a portable fallback.
    from rapidfuzz.fuzz import ratio as _rapidfuzz_ratio
    from rapidfuzz.process import extract as _rapidfuzz_extract
except ImportError:  # pragma: no cover - covered by the stdlib implementation
    _rapidfuzz_ratio = None
    _rapidfuzz_extract = None


_MISSING_TEXT = {"", "N/A", "NA", "NAN", "NONE", "NULL", "<NA>"}
_MIN_PAIR_BLOCK_SIZE = 2
_MAX_ALIAS_GROUP_SIZE = 4
_DEFAULTS: dict[str, Any] = {
    "fuzzy_threshold": 90.0,
    "parent_fuzzy_threshold": None,
    "enable_parent_aware_matching": True,
    "fuzzy_runner_up_margin": 5.0,
    "completion_year_tolerance": 0,
    "geography_distance_km": 1.0,
    "max_fuzzy_candidates_per_record": 3,
    "rule_version": RULE_VERSION,
}


def _params(params: Mapping[str, Any] | None) -> dict[str, Any]:
    """Combine defaults with flat or ``building_master_matching`` parameters."""
    value = dict(_DEFAULTS)
    supplied = dict(params or {})
    nested = supplied.get("building_master_matching")
    if isinstance(nested, Mapping):
        supplied = {**supplied, **nested}
    for key in value:
        if key in supplied and supplied[key] is not None:
            value[key] = supplied[key]
    if value["parent_fuzzy_threshold"] is None:
        value["parent_fuzzy_threshold"] = value["fuzzy_threshold"]
    return value


def _text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    result = str(value).strip()
    return "" if result.upper() in _MISSING_TEXT else result


def _key(value: Any) -> str:
    """Normalize a generic categorical value for conservative comparison."""
    return re.sub(r"[^A-Z0-9]+", "", _text(value).upper())


def _chinese_key(value: Any) -> str:
    """Normalize Chinese labels, keeping the digits and letters that carry
    building identity: house numbers, block letters and phase numbers."""
    return re.sub(r"[^\u3400-\u9fffA-Z0-9]+", "", _text(value).upper())


def _geography_key(value: Any) -> str:
    """Normalize geography aliases while dropping generic terminal labels."""
    tokens = re.sub(r"[^A-Z0-9]+", " ", _text(value).upper()).split()
    suffixes = {"DISTRICT", "REGION", "AREA", "ZONE", "LOCATION"}
    while tokens and tokens[-1] in suffixes:
        tokens.pop()
    return "".join(tokens)


def _geography_keys(value: Any) -> list[str]:
    """Return every locality a geography label names.

    Centaline stamps 61% of its estates with a composite label such as
    ``Tsuen Wan | Belvedere Garden``, which normalizes to one unusable key that
    matches nothing on the other side. Each component is a real locality.
    """
    text = _text(value)
    if not text:
        return []
    parts = [_geography_key(part) for part in text.split("|")]
    seen: list[str] = []
    for part in parts:
        if part and part not in seen:
            seen.append(part)
    return seen


def _normal_name(row: Mapping[str, Any]) -> str:
    return _key(row.get("normalized_name")) or _key(row.get("name_en"))


def _normal_chinese_name(row: Mapping[str, Any]) -> str:
    return _chinese_key(row.get("name_zh")) or _chinese_key(row.get("building_name_zh"))


def _normal_address(row: Mapping[str, Any]) -> str:
    return _key(row.get("normalized_address")) or _key(row.get("address_en"))


def _score(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    if _rapidfuzz_ratio is not None:
        return round(float(_rapidfuzz_ratio(left, right)), 3)
    return round(100.0 * SequenceMatcher(a=left, b=right).ratio(), 3)


def _same_text(left: Any, right: Any) -> bool:
    return bool(_key(left) and _key(left) == _key(right))


def _as_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(parsed) else parsed


def _distance_km(left: Mapping[str, Any], right: Mapping[str, Any]) -> float | None:
    lat1, lon1 = _as_float(left.get("latitude")), _as_float(left.get("longitude"))
    lat2, lon2 = _as_float(right.get("latitude")), _as_float(right.get("longitude"))
    if None in (lat1, lon1, lat2, lon2):
        return None
    radius = 6371.0088
    lat_delta = math.radians(lat2 - lat1)
    lon_delta = math.radians(lon2 - lon1)
    calculation = (
        math.sin(lat_delta / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(lon_delta / 2) ** 2
    )
    return radius * 2 * math.asin(math.sqrt(calculation))


def _geography_agrees(
    left: Mapping[str, Any], right: Mapping[str, Any], config: Mapping[str, Any]
) -> bool:
    """Require positive geographic evidence; missing geography is not agreement."""
    if _normal_address(left) and _normal_address(left) == _normal_address(right):
        return True
    # Compare every locality either side names, at either level. Blocking already
    # emits both levels and splits composite labels; agreement has to do the same
    # or the pair meets in a block and is then rejected here.
    left_geography = {
        key
        for field in ("subdistrict", "district")
        for key in _geography_keys(left.get(field))
    }
    right_geography = {
        key
        for field in ("subdistrict", "district")
        for key in _geography_keys(right.get(field))
    }
    if left_geography & right_geography:
        return True
    distance = _distance_km(left, right)
    return distance is not None and distance <= float(config["geography_distance_km"])


def _completion_agrees(
    left: Mapping[str, Any], right: Mapping[str, Any], config: Mapping[str, Any]
) -> bool:
    left_year, right_year = _as_float(left.get("completion_year")), _as_float(right.get("completion_year"))
    if left_year is None or right_year is None:
        return True
    return abs(left_year - right_year) <= float(config["completion_year_tolerance"])


def _property_type_agrees(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    left_type, right_type = _key(left.get("property_type")), _key(right.get("property_type"))
    return not left_type or not right_type or left_type == right_type


def _compatible(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    return (
        _text(left.get("source_system")) != _text(right.get("source_system"))
        and _text(left.get("source_domain")) == _text(right.get("source_domain"))
        and _text(left.get("entity_grain")) == _text(right.get("entity_grain"))
    )


def _candidate_row(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
    config: Mapping[str, Any],
    *,
    parent_supported: bool = False,
    bilingual_exact: bool = False,
) -> dict[str, Any] | None:
    """Build an eligible candidate, never crossing source/domain/grain boundaries."""
    if not _compatible(left, right):
        return None

    left_name, right_name = _normal_name(left), _normal_name(right)
    left_name_zh, right_name_zh = _normal_chinese_name(left), _normal_chinese_name(right)
    left_address, right_address = _normal_address(left), _normal_address(right)
    name_score, address_score = _score(left_name, right_name), _score(left_address, right_address)
    geography_agrees = _geography_agrees(left, right, config)
    completion_agrees = _completion_agrees(left, right, config)
    property_type_agrees = _property_type_agrees(left, right)
    exact_name = bool(
        (left_name and left_name == right_name)
        or (left_name_zh and left_name_zh == right_name_zh)
    )
    exact_address = bool(left_address and left_address == right_address)

    if bilingual_exact:
        tier, match_score, reason = (
            "exact_bilingual",
            100.0,
            "exact normalized English and Chinese names",
        )
    elif parent_supported and exact_name:
        tier, match_score, reason = (
            "exact_name_parent",
            100.0,
            "exact normalized name within a safe linked parent",
        )
    elif exact_name and exact_address:
        tier, match_score, reason = "exact_name_address", 100.0, "exact normalized name and address"
    elif exact_name and geography_agrees:
        tier, match_score, reason = "exact_name_geography", 100.0, "exact normalized name with geography"
    elif parent_supported and name_score >= float(config["parent_fuzzy_threshold"]):
        tier, match_score, reason = (
            "fuzzy_name_parent",
            name_score,
            "fuzzy name within a safe linked parent",
        )
    elif geography_agrees and name_score >= float(config["fuzzy_threshold"]):
        tier, match_score, reason = "fuzzy_name_geography", name_score, "fuzzy name with geography support"
    else:
        return None

    return {
        "left_source_record_id": _text(left["source_record_id"]),
        "right_source_record_id": _text(right["source_record_id"]),
        "source_domain": _text(left["source_domain"]),
        "entity_grain": _text(left["entity_grain"]),
        "match_tier": tier,
        "match_score": match_score,
        "name_score": name_score,
        "address_score": address_score,
        "geography_agrees": geography_agrees,
        "completion_year_agrees": completion_agrees,
        "property_type_agrees": property_type_agrees,
        "is_ambiguous": False,
        "decision_reason": reason,
    }


def _pair_key(left_id: str, right_id: str) -> tuple[str, str]:
    return tuple(sorted((left_id, right_id)))


def _is_generic_building_name(record: Mapping[str, Any]) -> bool:
    """Identify labels that are meaningful only in their estate/phase context."""
    if _key(record.get("entity_grain")) != "BUILDING":
        return False
    name = _normal_name(record)
    # Letter suffixes are as generic as numeric ones: "Block A" identifies a
    # building only inside its estate, and thousands of them share the label.
    return bool(
        re.fullmatch(
            r"(?:TOWER|BLOCK|BLK|HOUSE|MANSION|COURT|PHASE|SITE|CARPARK)[\dA-Z]{0,3}",
            name,
        )
    )


def _safe_parent_links(
    exact_candidates: Mapping[tuple[str, str], Mapping[str, Any]],
    records_by_id: Mapping[str, Mapping[str, Any]],
) -> dict[str, str]:
    """Return one-to-one estate/phase aliases supported by exact evidence only."""
    links: dict[str, str] = {}
    degree: defaultdict[str, int] = defaultdict(int)
    pairs: list[tuple[str, str]] = []
    for pair, candidate in exact_candidates.items():
        left, right = (records_by_id[source_id] for source_id in pair)
        if (
            _key(left["entity_grain"]) == "ESTATEPHASE"
            and _key(right["entity_grain"]) == "ESTATEPHASE"
            and candidate["completion_year_agrees"]
            and candidate["property_type_agrees"]
        ):
            pairs.append(pair)
            degree[pair[0]] += 1
            degree[pair[1]] += 1
    for left_id, right_id in pairs:
        if degree[left_id] == 1 and degree[right_id] == 1:
            links[left_id] = right_id
            links[right_id] = left_id
    return links


def _parent_supported_building_pairs(
    records: Iterable[Mapping[str, Any]], parent_links: Mapping[str, str]
) -> set[tuple[str, str]]:
    """Cross-pair children only when their source-local parents are safely linked."""
    parents_by_native: defaultdict[tuple[str, str, str], list[str]] = defaultdict(list)
    children_by_parent: defaultdict[str, list[str]] = defaultdict(list)
    for record in records:
        if _key(record["entity_grain"]) == "ESTATEPHASE" and _text(record["native_id"]):
            parents_by_native[
                (_text(record["source_system"]), _text(record["source_domain"]), _text(record["native_id"]))
            ].append(_text(record["source_record_id"]))
    for record in records:
        if _key(record["entity_grain"]) != "BUILDING" or not _text(record["native_parent_id"]):
            continue
        parent_ids = parents_by_native.get(
            (
                _text(record["source_system"]),
                _text(record["source_domain"]),
                _text(record["native_parent_id"]),
            ),
            [],
        )
        if len(parent_ids) == 1:
            children_by_parent[parent_ids[0]].append(_text(record["source_record_id"]))

    pairs: set[tuple[str, str]] = set()
    for parent_id, linked_parent_id in parent_links.items():
        if parent_id >= linked_parent_id:
            continue
        for left_id in children_by_parent[parent_id]:
            for right_id in children_by_parent[linked_parent_id]:
                pairs.add(_pair_key(left_id, right_id))
    return pairs


def _exact_block_keys(record: Mapping[str, Any]) -> list[tuple[str, str, str, str, str]]:
    """Return name-plus-positive-evidence exact blocks for a single record."""
    domain, grain = _text(record["source_domain"]), _text(record["entity_grain"])
    names = [
        ("en", name) for name in (_normal_name(record),) if name
    ] + [
        ("zh", name) for name in (_normal_chinese_name(record),) if name
    ]
    # Emit every level and every component rather than the finest single label:
    # the two sources cover subdistrict unevenly, and Centaline's composite
    # labels ("Tsuen Wan | Belvedere Garden") match nothing when kept whole.
    evidence = [("address", _normal_address(record))]
    evidence += [
        ("subdistrict", key) for key in _geography_keys(record.get("subdistrict"))
    ]
    evidence += [("district", key) for key in _geography_keys(record.get("district"))]
    return [
        (domain, grain, language, name, f"{kind}:{value}")
        for language, name in names
        for kind, value in evidence
        if value
    ]


def _bilingual_exact_block_key(record: Mapping[str, Any]) -> tuple[str, str, str, str] | None:
    """Return an exact bilingual alias key without requiring geography."""
    name_en, name_zh = _normal_name(record), _normal_chinese_name(record)
    if not name_en or not name_zh:
        return None
    return (
        _text(record["source_domain"]),
        _text(record["entity_grain"]),
        name_en,
        name_zh,
    )


def _fuzzy_block_key(record: Mapping[str, Any]) -> tuple[str, str, str, str] | None:
    """Return the strongest usable shared geography block for fuzzy retrieval."""
    domain, grain = _text(record["source_domain"]), _text(record["entity_grain"])
    subdistrict = _geography_key(record.get("subdistrict"))
    district = _geography_key(record.get("district"))
    address_prefix = _normal_address(record)[:12]
    if subdistrict:
        return domain, grain, "subdistrict", subdistrict
    if district:
        return domain, grain, "district", district
    if address_prefix:
        return domain, grain, "address_prefix", address_prefix
    return None


def _cross_source_pairs(
    indexes: list[int], records: list[dict[str, Any]]
) -> Iterable[tuple[int, int]]:
    """Yield only cross-source pairs inside one deterministic blocking group."""
    by_source: defaultdict[str, list[int]] = defaultdict(list)
    for index in indexes:
        by_source[_text(records[index]["source_system"])].append(index)
    systems = sorted(by_source)
    for position, left_system in enumerate(systems):
        for right_system in systems[position + 1 :]:
            for left_index in by_source[left_system]:
                for right_index in by_source[right_system]:
                    yield left_index, right_index


_FINGERPRINT_TIER = "transaction_fingerprint"
_CROSS_GRAIN_TIER = "cross_grain_alias"
# Tiers whose evidence stands on its own, so the same-grain / cross-source rule
# and the completion-year veto do not apply to them.
_RELAXED_TIERS = frozenset({_FINGERPRINT_TIER, _CROSS_GRAIN_TIER})


def _numeric_tokens(name: str) -> tuple[str, ...]:
    """Return the digit runs in a name, which carry building identity."""
    return tuple(re.findall(r"\d+", name))


def _top_fuzzy_matches(
    query_name: str,
    choices: tuple[list[str], list[str]],
    config: Mapping[str, Any],
) -> list[tuple[str, float]]:
    """Return deterministic top-N names, with a stdlib fallback for tests."""
    limit = int(config["max_fuzzy_candidates_per_record"])
    threshold = float(config["fuzzy_threshold"])
    if limit <= 0 or not query_name:
        return []
    choice_ids, choice_names = choices
    # Edit distance barely moves for a one-digit change, so "90 Sung Wong Toi
    # Road" scores 94 against "92 Sung Wong Toi Road". In street-named stock the
    # number is the identity, so require it to agree before scoring at all.
    query_digits = _numeric_tokens(query_name)
    filtered = [
        (choice_id, name)
        for choice_id, name in zip(choice_ids, choice_names)
        if _numeric_tokens(name) == query_digits
    ]
    if not filtered:
        return []
    choice_ids = [choice_id for choice_id, _ in filtered]
    choice_names = [name for _, name in filtered]
    if _rapidfuzz_extract is not None and _rapidfuzz_ratio is not None:
        extracted = _rapidfuzz_extract(
            query_name,
            choice_names,
            scorer=_rapidfuzz_ratio,
            score_cutoff=threshold,
            limit=limit,
        )
        matches = [(choice_ids[index], round(float(score), 3)) for _, score, index in extracted]
    else:
        matches = []
        for source_id, name in zip(choice_ids, choice_names):
            score = _score(query_name, name)
            if score >= threshold:
                matches.append((source_id, score))
        matches.sort(key=lambda value: (-value[1], value[0]))
        matches = matches[:limit]
    return sorted(matches, key=lambda value: (-value[1], value[0]))


def _contract_frame(rows: Iterable[dict[str, Any]], columns: list[str]) -> pd.DataFrame:
    frame = pd.DataFrame.from_records(list(rows))
    return frame.reindex(columns=columns)


def _validate_source_records(source_records: pd.DataFrame) -> pd.DataFrame:
    missing = set(SOURCE_RECORD_COLUMNS).difference(source_records.columns)
    if missing:
        raise ValueError(f"source_records missing required columns: {sorted(missing)}")
    records = source_records.reindex(columns=SOURCE_RECORD_COLUMNS).copy()
    if records["source_record_id"].isna().any() or records["source_record_id"].duplicated().any():
        raise ValueError("source_record_id must be present and unique")
    return records.sort_values("source_record_id", kind="stable").reset_index(drop=True)


def generate_match_candidates(  # noqa: PLR0912, PLR0915
    source_records: pd.DataFrame, params: Mapping[str, Any] | None = None
) -> pd.DataFrame:
    """Generate cross-source, domain/grain-compatible candidate pairs.

    Candidate generation deliberately does not merge anything.  Exact collision
    groups are flagged here so resolution can route every member to review.
    """
    records = _validate_source_records(source_records)
    config = _params(params)
    prepared_records = records.to_dict(orient="records")
    records_by_id = {
        _text(record["source_record_id"]): record for record in prepared_records
    }
    exact_blocks: defaultdict[tuple[str, str, str, str, str], list[int]] = defaultdict(list)
    bilingual_exact_blocks: defaultdict[tuple[str, str, str, str], list[int]] = defaultdict(list)
    fuzzy_blocks: defaultdict[tuple[str, str, str, str], list[int]] = defaultdict(list)
    for index, record in enumerate(prepared_records):
        for exact_key in _exact_block_keys(record):
            exact_blocks[exact_key].append(index)
        bilingual_key = _bilingual_exact_block_key(record)
        if bilingual_key is not None:
            bilingual_exact_blocks[bilingual_key].append(index)
        fuzzy_key = _fuzzy_block_key(record)
        if fuzzy_key is not None:
            fuzzy_blocks[fuzzy_key].append(index)

    rows: list[dict[str, Any]] = []
    exact_degree: defaultdict[str, int] = defaultdict(int)
    exact_by_pair: dict[tuple[str, str], dict[str, Any]] = {}
    for indexes in exact_blocks.values():
        if len(indexes) < _MIN_PAIR_BLOCK_SIZE:
            continue
        for left_index, right_index in _cross_source_pairs(indexes, prepared_records):
            candidate = _candidate_row(prepared_records[left_index], prepared_records[right_index], config)
            if candidate is None or not candidate["match_tier"].startswith("exact_"):
                continue
            pair = _pair_key(
                candidate["left_source_record_id"], candidate["right_source_record_id"]
            )
            if pair not in exact_by_pair:
                exact_by_pair[pair] = candidate
                exact_degree[pair[0]] += 1
                exact_degree[pair[1]] += 1

    for indexes in bilingual_exact_blocks.values():
        if len(indexes) < _MIN_PAIR_BLOCK_SIZE:
            continue
        for left_index, right_index in _cross_source_pairs(indexes, prepared_records):
            candidate = _candidate_row(
                prepared_records[left_index],
                prepared_records[right_index],
                config,
                bilingual_exact=True,
            )
            if candidate is None:
                continue
            pair = _pair_key(
                candidate["left_source_record_id"], candidate["right_source_record_id"]
            )
            if pair not in exact_by_pair:
                exact_degree[pair[0]] += 1
                exact_degree[pair[1]] += 1
            exact_by_pair[pair] = candidate

    parent_pairs: set[tuple[str, str]] = set()
    parent_candidates: dict[tuple[str, str], dict[str, Any]] = {}
    if bool(config["enable_parent_aware_matching"]):
        parent_links = _safe_parent_links(exact_by_pair, records_by_id)
        parent_pairs = _parent_supported_building_pairs(prepared_records, parent_links)
        for pair in parent_pairs:
            candidate = _candidate_row(
                records_by_id[pair[0]],
                records_by_id[pair[1]],
                config,
                parent_supported=True,
            )
            if candidate is not None:
                parent_candidates[pair] = candidate

    # Generic tower/block labels have no cross-estate meaning.  They are only
    # eligible through the safe parent alias links derived above.
    def _base_exact_candidate(
        pair: tuple[str, str], candidate: dict[str, Any]
    ) -> dict[str, Any] | None:
        left, right = records_by_id[pair[0]], records_by_id[pair[1]]
        if (
            (_is_generic_building_name(left) and _text(left["native_parent_id"]))
            or (_is_generic_building_name(right) and _text(right["native_parent_id"]))
        ):
            return parent_candidates.get(pair)
        return parent_candidates.get(pair, candidate)

    base_exact_by_pair = {
        pair: selected
        for pair, candidate in exact_by_pair.items()
        if (selected := _base_exact_candidate(pair, candidate)) is not None
    }
    rows.extend(base_exact_by_pair.values())
    rows.extend(
        candidate
        for pair, candidate in parent_candidates.items()
        if candidate["match_tier"] == "exact_name_parent" and pair not in base_exact_by_pair
    )
    safe_exact_records = {
        source_id
        for pair, candidate in exact_by_pair.items()
        if exact_degree[pair[0]] == 1
        and exact_degree[pair[1]] == 1
        and candidate["completion_year_agrees"]
        and candidate["property_type_agrees"]
        for source_id in pair
    }

    fuzzy_edges: list[tuple[float, str, str, dict[str, Any]]] = []
    for indexes in fuzzy_blocks.values():
        available = [
            index
            for index in indexes
            if _text(prepared_records[index]["source_record_id"]) not in safe_exact_records
            and _normal_name(prepared_records[index])
            and not (
                _is_generic_building_name(prepared_records[index])
                and _text(prepared_records[index]["native_parent_id"])
            )
        ]
        if len(available) < _MIN_PAIR_BLOCK_SIZE:
            continue
        by_source: defaultdict[str, list[int]] = defaultdict(list)
        for index in available:
            by_source[_text(prepared_records[index]["source_system"])].append(index)
        choices_by_source: dict[str, tuple[list[str], list[str]]] = {}
        for source_system in sorted(by_source):
            opposite_indexes = [
                index
                for other_source, source_indexes in by_source.items()
                if other_source != source_system
                for index in source_indexes
            ]
            choices_by_source[source_system] = (
                [_text(prepared_records[index]["source_record_id"]) for index in opposite_indexes],
                [_normal_name(prepared_records[index]) for index in opposite_indexes],
            )
        for source_system in sorted(by_source):
            choices = choices_by_source[source_system]
            for query_index in by_source[source_system]:
                query = prepared_records[query_index]
                query_id = _text(query["source_record_id"])
                for choice_id, _ in _top_fuzzy_matches(
                    _normal_name(query), choices, config
                ):
                    pair = _pair_key(query_id, choice_id)
                    if pair in base_exact_by_pair:
                        continue
                    candidate = _candidate_row(query, records_by_id[choice_id], config)
                    if (
                        candidate is not None
                        and candidate["match_tier"] == "fuzzy_name_geography"
                    ):
                        fuzzy_edges.append((float(candidate["match_score"]), *pair, candidate))

    # Parent-supported fuzzy candidates use the same bounded, deterministic
    # selection path as geography candidates.  They bypass geography only;
    # compatibility and later conflict checks remain unchanged.
    for pair, candidate in parent_candidates.items():
        if candidate["match_tier"] == "fuzzy_name_parent":
            fuzzy_edges.append((float(candidate["match_score"]), *pair, candidate))

    max_fuzzy = int(config["max_fuzzy_candidates_per_record"])
    fuzzy_degree: defaultdict[str, int] = defaultdict(int)
    seen_pairs = set(base_exact_by_pair) | {
        pair
        for pair, candidate in parent_candidates.items()
        if candidate["match_tier"] == "exact_name_parent"
    }
    for _, left_id, right_id, candidate in sorted(
        fuzzy_edges, key=lambda value: (-value[0], value[1], value[2])
    ):
        pair = (left_id, right_id)
        if (
            pair in seen_pairs
            or fuzzy_degree[left_id] >= max_fuzzy
            or fuzzy_degree[right_id] >= max_fuzzy
        ):
            continue
        seen_pairs.add(pair)
        fuzzy_degree[left_id] += 1
        fuzzy_degree[right_id] += 1
        rows.append(candidate)

    candidates = _contract_frame(rows, CANDIDATE_PAIR_COLUMNS)
    if candidates.empty:
        return candidates

    degrees: defaultdict[str, int] = defaultdict(int)
    for row in candidates.itertuples(index=False):
        degrees[row.left_source_record_id] += 1
        degrees[row.right_source_record_id] += 1
    candidates["is_ambiguous"] = [
        degrees[left] > 1 or degrees[right] > 1
        for left, right in zip(
            candidates["left_source_record_id"], candidates["right_source_record_id"]
        )
    ]
    candidates.loc[candidates["is_ambiguous"], "decision_reason"] = (
        candidates.loc[candidates["is_ambiguous"], "decision_reason"] + "; collision candidate"
    )
    return candidates.sort_values(
        ["left_source_record_id", "right_source_record_id"], kind="stable"
    ).reset_index(drop=True)


class _DisjointSet:
    def __init__(
        self, values: Iterable[str], locks: Mapping[str, str] | None = None
    ) -> None:
        self.parent = {value: value for value in values}
        self.root_locks = {
            value: {locks[value]} if locks and value in locks else set()
            for value in self.parent
        }

    def find(self, value: str) -> str:
        while self.parent[value] != value:
            self.parent[value] = self.parent[self.parent[value]]
            value = self.parent[value]
        return value

    def locks_for(self, value: str) -> set[str]:
        return self.root_locks[self.find(value)]

    def union(self, left: str, right: str) -> str:
        left_root, right_root = self.find(left), self.find(right)
        if left_root == right_root:
            return left_root
        root, child = min(left_root, right_root), max(left_root, right_root)
        self.parent[child] = root
        self.root_locks[root].update(self.root_locks.pop(child))
        return root


def _rejected_pairs(
    reviewed_overrides: pd.DataFrame | None, source_ids: set[str]
) -> set[tuple[str, str]]:
    """Return pairs a reviewer has ruled out as different buildings.

    A rejection is recorded as two rows sharing one proposed canonical id, both
    marked "rejected". Without this the verdict is silently discarded and the
    pair returns to the review queue on every run.
    """
    if reviewed_overrides is None or reviewed_overrides.empty:
        return set()
    if not {"source_record_id", "canonical_building_id", "review_status"}.issubset(
        reviewed_overrides.columns
    ):
        return set()
    rejected = reviewed_overrides.loc[
        reviewed_overrides["review_status"].map(_key).eq("REJECTED")
    ]
    forbidden: set[tuple[str, str]] = set()
    for _, group in rejected.groupby(
        reviewed_overrides["canonical_building_id"].map(_text), dropna=False
    ):
        members = sorted(
            {
                _text(value)
                for value in group["source_record_id"]
                if _text(value) in source_ids
            }
        )
        for index, left in enumerate(members):
            for right in members[index + 1 :]:
                forbidden.add((left, right))
    return forbidden


def _label_identity(record: Mapping[str, Any]) -> tuple[str, ...]:
    """Return the digit runs in a record's name, which label a block or tower."""
    return _numeric_tokens(_normal_name(record))


def _uniquely_best_option(
    options: list[dict[str, Any]],
    candidate: Mapping[str, Any],
    by_id: Mapping[str, Mapping[str, Any]],
    side: str,
) -> bool:
    """Is this candidate the only one whose block label matches the record's?

    "Block 41" arrives with 27 candidates — every block of the same estate —
    and the collision guard rejects all of them, the correct "Block 41"
    included. When exactly one candidate carries the same digits, the ambiguity
    is only apparent and that candidate is the answer.
    """
    anchor_id = candidate[f"{side}_source_record_id"]
    other_side = "right" if side == "left" else "left"
    anchor_labels = _label_identity(by_id[anchor_id])
    if not anchor_labels:
        return False
    agreeing = [
        option
        for option in options
        if _label_identity(by_id[option[f"{other_side}_source_record_id"]])
        == anchor_labels
    ]
    if len(agreeing) != 1:
        return False
    return (
        agreeing[0][f"{other_side}_source_record_id"]
        == candidate[f"{other_side}_source_record_id"]
    )


def _approved_overrides(
    reviewed_overrides: pd.DataFrame | None, source_ids: set[str]
) -> tuple[dict[str, str], set[str]]:
    if reviewed_overrides is None or reviewed_overrides.empty:
        return {}, set()
    missing = {"source_record_id", "canonical_building_id", "review_status"}.difference(
        reviewed_overrides.columns
    )
    if missing:
        raise ValueError(f"reviewed_overrides missing required columns: {sorted(missing)}")
    approved = reviewed_overrides.loc[
        reviewed_overrides["review_status"].map(_key).eq("APPROVED")
    ].reindex(columns=OVERRIDE_COLUMNS)
    locks: dict[str, str] = {}
    conflicts: set[str] = set()
    for row in approved.sort_values(["source_record_id", "canonical_building_id"]).itertuples(index=False):
        source_id, canonical_id = _text(row.source_record_id), _text(row.canonical_building_id)
        if not source_id or source_id not in source_ids or not canonical_id:
            continue
        existing = locks.get(source_id)
        if existing and existing != canonical_id:
            conflicts.add(source_id)
        else:
            locks[source_id] = canonical_id
    for source_id in conflicts:
        locks.pop(source_id, None)
    return locks, conflicts


def _review_row(
    source: Mapping[str, Any],
    candidate: Mapping[str, Any],
    score: float,
    runner_up: float | None,
    reason: str,
) -> dict[str, Any]:
    source_id, candidate_id = _text(source["source_record_id"]), _text(candidate["source_record_id"])
    review_id = hashlib.sha256(f"{source_id}|{candidate_id}|{reason}".encode()).hexdigest()[:20]
    return {
        "review_id": f"review-{review_id}",
        "source_record_id": source_id,
        "candidate_source_record_id": candidate_id,
        "source_system": source.get("source_system"),
        "source_domain": source.get("source_domain"),
        "entity_grain": source.get("entity_grain"),
        "source_name_en": source.get("name_en"),
        "source_name_zh": source.get("name_zh"),
        "source_address": source.get("address_en"),
        "source_district": source.get("district"),
        "source_completion_year": source.get("completion_year"),
        "candidate_name_en": candidate.get("name_en"),
        "candidate_name_zh": candidate.get("name_zh"),
        "candidate_address": candidate.get("address_en"),
        "candidate_district": candidate.get("district"),
        "candidate_completion_year": candidate.get("completion_year"),
        "match_score": score,
        "runner_up_score": runner_up,
        "reason": reason,
        "review_status": "pending",
    }


def _stable_canonical_id(source_ids: Iterable[str]) -> str:
    fingerprint = "|".join(sorted(source_ids))
    return f"building-{hashlib.sha256(fingerprint.encode('utf-8')).hexdigest()[:20]}"


def _best_record(records: list[Mapping[str, Any]]) -> Mapping[str, Any]:
    """Choose a stable representative with the most populated canonical fields."""
    fields = [
        "name_en", "name_zh", "address_en", "address_zh", "district", "subdistrict",
        "completion_year", "property_type", "latitude", "longitude",
    ]
    return min(
        records,
        key=lambda record: (
            -sum(bool(_text(record.get(field))) for field in fields),
            _text(record["source_record_id"]),
        ),
    )


def _join_values(values: Iterable[Any]) -> Any:
    cleaned = sorted({_text(value) for value in values if _text(value)})
    return "; ".join(cleaned) if cleaned else pd.NA


_COORDINATE_FIELDS = ("latitude", "longitude")


def _coalesce(
    ordered: list[Mapping[str, Any]], field: str
) -> Any:
    """Take the first populated value across the merged group.

    The point of matching two sources is that they complete each other:
    Centaline carries the address and no Chinese name, Midland the reverse.
    Reading every field off one representative record throws half of that away.
    """
    for record in ordered:
        value = record.get(field)
        if _text(value):
            return value
    return pd.NA


def _coalesce_coordinates(ordered: list[Mapping[str, Any]]) -> tuple[Any, Any]:
    """Latitude and longitude come from one record or neither, never mixed."""
    for record in ordered:
        if _text(record.get("latitude")) and _text(record.get("longitude")):
            return record["latitude"], record["longitude"]
    return pd.NA, pd.NA


def _canonical_row(
    group: list[Mapping[str, Any]],
    canonical_id: str,
    bridge_group: list[Mapping[str, Any]],
) -> dict[str, Any]:
    representative = _best_record(group)
    # Representative first, then the rest in id order, so the result is stable.
    ordered = [representative] + sorted(
        (record for record in group if record is not representative),
        key=lambda record: _text(record["source_record_id"]),
    )
    latitude, longitude = _coalesce_coordinates(ordered)
    centaline = [record for record in group if _key(record["source_system"]) == "CENTALINE"]
    midland = [record for record in group if _key(record["source_system"]) == "MIDLAND"]
    matched = any(_text(record["match_tier"]) != "singleton" for record in bridge_group)
    return {
        "canonical_building_id": canonical_id,
        "canonical_complex_id": pd.NA,
        "canonical_phase_id": pd.NA,
        "entity_grain": representative["entity_grain"],
        "source_domain": representative["source_domain"],
        "canonical_name_en": _coalesce(ordered, "name_en"),
        "canonical_name_zh": _coalesce(ordered, "name_zh"),
        "complex_name_en": _coalesce(ordered, "complex_name_en"),
        "complex_name_zh": _coalesce(ordered, "complex_name_zh"),
        "phase_name_en": _coalesce(ordered, "phase_name_en"),
        "phase_name_zh": _coalesce(ordered, "phase_name_zh"),
        "building_name_en": _coalesce(ordered, "building_name_en"),
        "building_name_zh": _coalesce(ordered, "building_name_zh"),
        "address_en": _coalesce(ordered, "address_en"),
        "address_zh": _coalesce(ordered, "address_zh"),
        "region": _coalesce(ordered, "region"),
        "district": _coalesce(ordered, "district"),
        "subdistrict": _coalesce(ordered, "subdistrict"),
        "latitude": latitude,
        "longitude": longitude,
        "completion_year": _coalesce(ordered, "completion_year"),
        "age": _coalesce(ordered, "age"),
        "property_type": _coalesce(ordered, "property_type"),
        "source_count": len(group),
        "centaline_name_en": _join_values(record["name_en"] for record in centaline),
        "centaline_name_zh": _join_values(record["name_zh"] for record in centaline),
        "centaline_native_ids": _join_values(record["native_id"] for record in centaline),
        "midland_name_en": _join_values(record["name_en"] for record in midland),
        "midland_name_zh": _join_values(record["name_zh"] for record in midland),
        "midland_native_ids": _join_values(record["native_id"] for record in midland),
        "resolution_tier": _join_values(record["match_tier"] for record in bridge_group),
        "match_confidence": max(float(record["match_score"]) for record in bridge_group),
        "review_status": "approved" if any(
            "override" in _text(record["decision_provenance"]) for record in bridge_group
        ) else "unreviewed" if matched else "singleton",
        "attribute_provenance": _join_values(
            record["attribute_provenance"] for record in group
        ),
    }


def _parent_lookup(
    records: Iterable[Mapping[str, Any]], source_to_canonical: Mapping[str, str]
) -> dict[tuple[str, str, str], set[str]]:
    """Map source-local estate/phase native IDs to resolved canonical IDs."""
    lookup: defaultdict[tuple[str, str, str], set[str]] = defaultdict(set)
    for record in records:
        if _key(record["entity_grain"]) != "ESTATEPHASE":
            continue
        native_id = _text(record["native_id"])
        if native_id:
            lookup[
                (_text(record["source_system"]), _text(record["source_domain"]), native_id)
            ].add(source_to_canonical[_text(record["source_record_id"])])
    return dict(lookup)


def _resolved_parent_ids(
    members: Iterable[Mapping[str, Any]],
    lookup: Mapping[tuple[str, str, str], set[str]],
    *,
    require_every_member: bool,
) -> set[str] | None:
    """Return unambiguous parents, or ``None`` where a required alias is absent."""
    parent_ids: set[str] = set()
    for member in members:
        parent_native_id = _text(member["native_parent_id"])
        if not parent_native_id:
            if require_every_member:
                return None
            continue
        matches = lookup.get(
            (_text(member["source_system"]), _text(member["source_domain"]), parent_native_id),
            set(),
        )
        if len(matches) != 1:
            if require_every_member:
                return None
            continue
        parent_ids.update(matches)
    return parent_ids


def _apply_hierarchy(
    canonical: pd.DataFrame, records: pd.DataFrame, bridge: pd.DataFrame
) -> pd.DataFrame:
    """Populate complex and phase IDs from source-local estate parent aliases."""
    result = canonical.copy()
    source_to_canonical = dict(
        zip(bridge["source_record_id"], bridge["canonical_building_id"])
    )
    members_by_canonical: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    record_maps = records.to_dict(orient="records")
    for record in record_maps:
        members_by_canonical[
            source_to_canonical[_text(record["source_record_id"])]
        ].append(record)
    lookup = _parent_lookup(record_maps, source_to_canonical)
    complex_by_phase: dict[str, str] = {}

    estate_rows = result.loc[result["entity_grain"].map(_key).eq("ESTATEPHASE")]
    for index, row in estate_rows.iterrows():
        canonical_id = row["canonical_building_id"]
        parent_ids = _resolved_parent_ids(
            members_by_canonical[canonical_id], lookup, require_every_member=False
        )
        if parent_ids is not None and len(parent_ids) == 1:
            parent_id = next(iter(parent_ids))
            if parent_id != canonical_id:
                result.loc[index, "canonical_complex_id"] = parent_id
                result.loc[index, "canonical_phase_id"] = canonical_id
                complex_by_phase[canonical_id] = parent_id
                continue
        result.loc[index, "canonical_complex_id"] = canonical_id
        result.loc[index, "canonical_phase_id"] = pd.NA
        complex_by_phase[canonical_id] = canonical_id

    building_rows = result.loc[result["entity_grain"].map(_key).eq("BUILDING")]
    for index, row in building_rows.iterrows():
        parent_ids = _resolved_parent_ids(
            members_by_canonical[row["canonical_building_id"]],
            lookup,
            require_every_member=True,
        )
        if parent_ids is None or len(parent_ids) != 1:
            continue
        phase_id = next(iter(parent_ids))
        result.loc[index, "canonical_phase_id"] = phase_id
        result.loc[index, "canonical_complex_id"] = complex_by_phase.get(
            phase_id, phase_id
        )
    return result.reindex(columns=CANONICAL_COLUMNS)


def resolve_canonical_buildings(  # noqa: PLR0912, PLR0915
    source_records: pd.DataFrame,
    candidate_pairs: pd.DataFrame,
    reviewed_overrides: pd.DataFrame | None,
    params: Mapping[str, Any] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Resolve only unambiguous candidates and preserve all other source rows.

    Approved overrides are applied first.  The remaining automatic edges must
    be mutually unique, compatible, conflict-free, and (for fuzzy pairs) clear
    of the configured runner-up margin.
    """
    records = _validate_source_records(source_records)
    config = _params(params)
    missing = set(CANDIDATE_PAIR_COLUMNS).difference(candidate_pairs.columns)
    if missing:
        raise ValueError(f"candidate_pairs missing required columns: {sorted(missing)}")
    candidates = candidate_pairs.reindex(columns=CANDIDATE_PAIR_COLUMNS).copy()
    record_maps = records.to_dict(orient="records")
    by_id = {_text(record["source_record_id"]): record for record in record_maps}
    source_ids = set(by_id)
    locks, override_conflicts = _approved_overrides(reviewed_overrides, source_ids)
    forbidden_pairs = _rejected_pairs(reviewed_overrides, source_ids)

    valid_candidates: list[dict[str, Any]] = []
    rejected: list[tuple[Mapping[str, Any], Mapping[str, Any], float, float | None, str]] = []
    for candidate in candidates.itertuples(index=False):
        left_id, right_id = _text(candidate.left_source_record_id), _text(candidate.right_source_record_id)
        if left_id not in source_ids or right_id not in source_ids or left_id == right_id:
            continue
        left, right = by_id[left_id], by_id[right_id]
        if tuple(sorted((left_id, right_id))) in forbidden_pairs:
            rejected.append((left, right, float(candidate.match_score), None, "reviewer rejected this pair"))
            continue
        if candidate.match_tier not in _RELAXED_TIERS and not _compatible(left, right):
            rejected.append((left, right, float(candidate.match_score), None, "domain or entity-grain conflict"))
            continue
        # Hundreds of shared transactions outweigh a completion-year disagreement:
        # the two agencies differ on roughly 5% of buildings' years, and that gate
        # would veto pairs the transactions themselves confirm.
        if candidate.match_tier not in _RELAXED_TIERS and (
            not bool(candidate.completion_year_agrees)
            or not bool(candidate.property_type_agrees)
        ):
            rejected.append((left, right, float(candidate.match_score), None, "completion year or property type conflict"))
            continue
        valid_candidates.append(candidate._asdict())

    ranked: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for candidate in valid_candidates:
        ranked[candidate["left_source_record_id"]].append(candidate)
        ranked[candidate["right_source_record_id"]].append(candidate)
    for values in ranked.values():
        values.sort(key=lambda row: (-float(row["match_score"]), row["left_source_record_id"], row["right_source_record_id"]))

    dsu = _DisjointSet(source_ids, locks)
    # A reviewed canonical ID is explicit linkage even where an automatic
    # candidate would not have been safe enough to create that linkage.
    locked_groups: defaultdict[str, list[str]] = defaultdict(list)
    for source_id, canonical_id in locks.items():
        locked_groups[canonical_id].append(source_id)
    for source_group in locked_groups.values():
        for source_id in source_group[1:]:
            dsu.union(source_group[0], source_id)

    selected_pairs: set[tuple[str, str]] = set()
    for candidate in sorted(
        valid_candidates,
        key=lambda row: (-float(row["match_score"]), row["left_source_record_id"], row["right_source_record_id"]),
    ):
        left_id, right_id = candidate["left_source_record_id"], candidate["right_source_record_id"]
        left_options, right_options = ranked[left_id], ranked[right_id]
        left_runner = float(left_options[1]["match_score"]) if len(left_options) > 1 else None
        right_runner = float(right_options[1]["match_score"]) if len(right_options) > 1 else None
        runner_up = max(value for value in (left_runner, right_runner) if value is not None) if left_runner is not None or right_runner is not None else None
        # A crowd of candidates is not real ambiguity when the block labels single
        # one out: the estate's other blocks carry different numbers.
        label_resolves = all(
            len(options) <= 1
            or _uniquely_best_option(options, candidate, by_id, side)
            for options, side in ((left_options, "left"), (right_options, "right"))
        )
        collision = (
            len(left_options) > 1 or len(right_options) > 1 or bool(candidate["is_ambiguous"])
        ) and not label_resolves
        fuzzy_margin_failed = (
            candidate["match_tier"] in {"fuzzy_name_geography", "fuzzy_name_parent"}
            and runner_up is not None
            and float(candidate["match_score"]) - runner_up < float(config["fuzzy_runner_up_margin"])
        )
        lock_conflict = (
            bool(locks)
            and len(dsu.locks_for(left_id) | dsu.locks_for(right_id)) > 1
        )
        if collision or fuzzy_margin_failed or lock_conflict:
            reason = "ambiguous candidate collision" if collision else "fuzzy runner-up margin not met" if fuzzy_margin_failed else "conflicting approved overrides"
            rejected.append(
                (by_id[left_id], by_id[right_id], float(candidate["match_score"]), runner_up, reason)
            )
            continue
        dsu.union(left_id, right_id)
        selected_pairs.add(tuple(sorted((left_id, right_id))))

    # One building is frequently recorded twice at different grains: Centaline
    # lists 1005 Canton Road as an estate_phase and again as a building. Those
    # two never meet as candidates, because matching only ever compares records
    # of the same grain. Uniting them here, after candidate resolution, adds the
    # edge without giving any record an extra candidate for the collision guard
    # to trip over. Requiring name and address to match exactly keeps
    # multi-block estates out: their buildings are named "Tower 1", not after
    # the estate.
    if bool(config.get("merge_cross_grain_aliases", True)):
        by_identity: defaultdict[tuple[str, str], list[str]] = defaultdict(list)
        for source_id in sorted(source_ids):
            record = by_id[source_id]
            name = _normal_name(record)
            if not name:
                continue
            # Address is the stronger context, but neither source records one for
            # a large share of this stock, and there the name is itself a street
            # address ("10-12 Fung Yi Street"). Fall back to the district.
            contexts = [_normal_address(record)] + [
                f"district:{key}" for key in _geography_keys(record.get("district"))
            ]
            for context in contexts:
                if context:
                    by_identity[(name, context)].append(source_id)
        for members in by_identity.values():
            grains = {_text(by_id[member]["entity_grain"]) for member in members}
            if len(members) < _MIN_PAIR_BLOCK_SIZE or len(grains) < _MIN_PAIR_BLOCK_SIZE:
                continue
            # A name shared by many records in one district is a generic label
            # ("Tower 1"), not an identity, so it cannot carry a merge.
            if len(members) > _MAX_ALIAS_GROUP_SIZE:
                continue
            for other in members[1:]:
                if tuple(sorted((members[0], other))) in forbidden_pairs:
                    continue
                dsu.union(members[0], other)

    # An override establishes the locked canonical ID, and candidates may join
    # it only if another approved lock does not name a different canonical row.
    groups: defaultdict[str, list[str]] = defaultdict(list)
    for source_id in sorted(source_ids):
        groups[dsu.find(source_id)].append(source_id)
    canonical_for_source: dict[str, str] = {}
    canonical_groups: dict[str, list[str]] = {}
    for root, source_group in groups.items():
        group_locks = dsu.root_locks[root]
        canonical_id = (
            sorted(group_locks)[0]
            if len(group_locks) == 1
            else _stable_canonical_id(source_group)
        )
        canonical_groups[canonical_id] = source_group
        for source_id in source_group:
            canonical_for_source[source_id] = canonical_id

    pair_metadata: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for candidate in valid_candidates:
        pair = tuple(sorted((candidate["left_source_record_id"], candidate["right_source_record_id"])))
        if pair in selected_pairs:
            pair_metadata[candidate["left_source_record_id"]].append(candidate)
            pair_metadata[candidate["right_source_record_id"]].append(candidate)

    bridge_rows: list[dict[str, Any]] = []
    bridge_by_source: dict[str, dict[str, Any]] = {}
    for source_id in sorted(source_ids):
        record = by_id[source_id]
        metadata = pair_metadata[source_id]
        best = sorted(metadata, key=lambda row: -float(row["match_score"]))[0] if metadata else None
        scores = [float(item["match_score"]) for item in ranked.get(source_id, [])]
        runner_up = sorted(scores, reverse=True)[1] if len(scores) > 1 else pd.NA
        overridden = source_id in locks
        bridge_row = {
                "source_record_id": source_id,
                "source_system": record["source_system"],
                "source_domain": record["source_domain"],
                "native_id": record["native_id"],
                "entity_grain": record["entity_grain"],
                "canonical_building_id": canonical_for_source[source_id],
                "match_tier": "override" if overridden else best["match_tier"] if best else "singleton",
                "match_score": 100.0 if overridden else float(best["match_score"]) if best else 0.0,
                "runner_up_score": runner_up,
                "candidate_count": len(ranked.get(source_id, [])),
                "decision_provenance": "reviewed_override" if overridden else "automatic_conservative_match" if best else "unmatched_singleton",
                "rule_version": str(config["rule_version"]),
                "source_name_en": record["name_en"],
                "source_name_zh": record["name_zh"],
                "source_address": record["address_en"],
                "source_url": record["source_url"],
        }
        bridge_rows.append(bridge_row)
        bridge_by_source[source_id] = bridge_row
    bridge = _contract_frame(bridge_rows, BRIDGE_COLUMNS)

    review_rows = [
        _review_row(left, right, score, runner_up, reason)
        for left, right, score, runner_up, reason in rejected
    ]
    for source_id in sorted(override_conflicts):
        review_rows.append(
            _review_row(
                by_id[source_id],
                by_id[source_id],
                0.0,
                None,
                "conflicting approved overrides",
            )
        )
    review = _contract_frame(review_rows, REVIEW_COLUMNS).drop_duplicates(
        subset=["review_id"], keep="first"
    ).sort_values("review_id", kind="stable").reset_index(drop=True)

    canonical_rows: list[dict[str, Any]] = []
    for canonical_id in sorted(canonical_groups):
        source_group = canonical_groups[canonical_id]
        canonical_rows.append(
            _canonical_row(
                [by_id[source_id] for source_id in source_group],
                canonical_id,
                [bridge_by_source[source_id] for source_id in source_group],
            )
        )
    canonical = _contract_frame(canonical_rows, CANONICAL_COLUMNS).sort_values(
        "canonical_building_id", kind="stable"
    ).reset_index(drop=True)
    canonical = _apply_hierarchy(canonical, records, bridge)
    return canonical, bridge, review


def build_building_master_outputs(
    canonical_master: pd.DataFrame,
    source_bridge: pd.DataFrame,
    review_queue: pd.DataFrame,
    candidate_pairs: pd.DataFrame,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    """Return workbook-ready sheets and a small reproducible resolution audit."""
    review_columns = [
        *REVIEW_COLUMNS,
        *[column for column in review_queue.columns if column not in REVIEW_COLUMNS],
    ]
    sheets = {
        "Canonical Buildings": sanitize_dataframe_content(
            canonical_master.reindex(columns=CANONICAL_COLUMNS)
        ),
        "Source Aliases": sanitize_dataframe_content(
            source_bridge.reindex(columns=BRIDGE_COLUMNS)
        ),
        "Review Queue": sanitize_dataframe_content(
            review_queue.reindex(columns=review_columns)
        ),
        "Candidate Pairs": sanitize_dataframe_content(
            candidate_pairs.reindex(columns=CANDIDATE_PAIR_COLUMNS)
        ),
    }
    bridge = sheets["Source Aliases"]
    audit_rows = [
        {"metric": "source_records", "value": len(bridge)},
        {"metric": "canonical_buildings", "value": len(sheets["Canonical Buildings"])},
        {"metric": "review_queue_rows", "value": len(sheets["Review Queue"])},
        {"metric": "candidate_pairs", "value": len(sheets["Candidate Pairs"])},
    ]
    for tier, count in bridge["match_tier"].value_counts(dropna=False).sort_index().items():
        audit_rows.append({"metric": f"bridge_tier:{tier}", "value": int(count)})
    return sheets, pd.DataFrame(audit_rows, columns=["metric", "value"])


def build_commercial_compatibility_master(
    canonical_master: pd.DataFrame,
) -> pd.DataFrame:
    """Expose the new commercial rows using the legacy consolidated schema."""
    commercial = canonical_master.loc[
        canonical_master["source_domain"].eq("commercial")
    ].copy()
    source_systems = (
        commercial["centaline_native_ids"].notna().map(
            {True: "source_a_commercial", False: ""}
        )
        + commercial["midland_native_ids"].notna().map(
            {True: ";source_b_commercial", False: ""}
        )
    ).str.strip(";")
    return pd.DataFrame(
        {
            "canonical_building_name": commercial["canonical_name_en"],
            "normalized_name": commercial["canonical_name_en"].map(
                normalize_building_name
            ),
            "district_name_en": commercial["district"],
            "zone_en": commercial["region"],
            "source_systems": source_systems,
            "native_source_count": commercial["source_count"],
            "manual_source_count": commercial["review_status"].eq("approved").astype(int),
            "has_manual_record": commercial["review_status"].eq("approved"),
            "preferred_completion_year": commercial["completion_year"],
            "preferred_address": commercial["address_en"],
            "preferred_management_company": pd.NA,
            "preferred_grade": pd.NA,
            "provenance": commercial["attribute_provenance"],
        }
    ).reset_index(drop=True)
