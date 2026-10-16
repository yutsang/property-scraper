"""Pure source adapters for consolidated building-master source records."""

from __future__ import annotations

import ast
import hashlib
import json
import re
from collections.abc import Iterable, Mapping, Sequence
from datetime import date
from typing import Any

import pandas as pd

from ...utils.building_supplement import normalize_building_name
from .contracts import SOURCE_RECORD_COLUMNS

_MISSING_TEXT = {"", "N/A", "NA", "NAN", "NONE", "NULL", "<NA>"}
_COORDINATE_KEYS = {"lat", "latitude", "lon", "lng", "long", "longitude"}
_MINIMUM_COORDINATE_KEYS = 2
_COORDINATE_PAIR = re.compile(
    r"^\s*\(?\s*(?:lat(?:itude)?\s*[:=]\s*)?[-+]?\d+(?:\.\d+)?"
    r"\s*(?:,|/|\s+)\s*(?:lon(?:g(?:itude)?)?\s*[:=]\s*)?"
    r"[-+]?\d+(?:\.\d+)?\s*\)?\s*$",
    re.IGNORECASE,
)
# Source names carry stray control characters (\x1a between name fragments).
# Excel rejects them outright, so a single such record fails the whole run.
_CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f]")


def _clean_text(value: Any) -> Any:
    """Return a stripped value or ``pd.NA`` for blank source values."""
    if value is None or isinstance(value, Mapping) or pd.isna(value):
        return pd.NA
    text = re.sub(r"\s+", " ", _CONTROL_CHARS.sub(" ", str(value))).strip()
    return pd.NA if text.upper() in _MISSING_TEXT else text


def _is_coordinate_mapping(value: Mapping[Any, Any]) -> bool:
    """Return whether a mapping contains only latitude/longitude coordinates."""
    keys = {str(key).strip().lower().replace("_", "") for key in value}
    return len(keys) >= _MINIMUM_COORDINATE_KEYS and keys.issubset(_COORDINATE_KEYS)


def _clean_address(value: Any) -> Any:
    """Reject coordinate-only values while retaining real English and CJK addresses."""
    if isinstance(value, Mapping):
        return pd.NA
    cleaned = _clean_text(value)
    if pd.isna(cleaned):
        return pd.NA
    text = str(cleaned)
    if _COORDINATE_PAIR.fullmatch(text):
        return pd.NA
    if text.startswith("{") and text.endswith("}"):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            try:
                parsed = ast.literal_eval(text)
            except (SyntaxError, ValueError):
                parsed = None
        if isinstance(parsed, Mapping) and _is_coordinate_mapping(parsed):
            return pd.NA
    return text


def _series(frame: pd.DataFrame, *columns: str) -> pd.Series:
    """Coalesce known source-column variants without changing the input frame."""
    result = pd.Series(pd.NA, index=frame.index, dtype="object")
    for column in columns:
        if column in frame.columns:
            values = frame[column].map(_clean_text)
            result = result.where(result.notna(), values)
    return result


def _address_series(frame: pd.DataFrame, *columns: str) -> pd.Series:
    """Coalesce address variants after excluding coordinate-only source values."""
    result = pd.Series(pd.NA, index=frame.index, dtype="object")
    for column in columns:
        if column in frame.columns:
            values = frame[column].map(_clean_address)
            result = result.where(result.notna(), values)
    return result


def _normalized_address(value: Any) -> str:
    """Normalize an address conservatively for matching and stable fingerprints."""
    cleaned = _clean_address(value)
    if pd.isna(cleaned):
        return ""
    normalized = str(cleaned).upper()
    normalized = re.sub(
        r"[#＃,.．，、;；:：/／\\()（）\[\]{}\-－–—]+", " ", normalized
    )
    return re.sub(r"\s+", " ", normalized).strip()


def _completion_year(*values: Any) -> Any:
    """Extract the first plausible completion year, including planned buildings."""
    for value in values:
        if value is None or pd.isna(value):
            continue
        match = re.search(r"\b(1[89]\d{2}|20\d{2}|21\d{2}|2200)\b", str(value))
        if match:
            return int(match.group(1))
    return pd.NA


def _age(completion_year: Any) -> Any:
    if completion_year is pd.NA or pd.isna(completion_year):
        return pd.NA
    return max(date.today().year - int(completion_year), 0)


def _stable_id(
    source_system: str,
    source_domain: str,
    entity_grain: str,
    native_id: Any,
    fingerprint_parts: Iterable[Any],
) -> tuple[str, str]:
    """Build a stable ID, falling back to a content fingerprint when necessary."""
    cleaned_id = _clean_text(native_id)
    if pd.notna(cleaned_id):
        identity = str(cleaned_id)
    else:
        fingerprint = "|".join(
            str(part) for part in fingerprint_parts if part is not None and not pd.isna(part)
        )
        identity = f"fp-{hashlib.sha256(fingerprint.encode('utf-8')).hexdigest()[:20]}"
    return f"{source_system}::{source_domain}::{entity_grain}::{identity}", identity


def _empty_records() -> pd.DataFrame:
    return pd.DataFrame(columns=SOURCE_RECORD_COLUMNS)


def _finalize(records: Sequence[dict[str, Any]]) -> pd.DataFrame:
    """Apply contract ordering, normalize nullable values, and enforce uniqueness."""
    if not records:
        return _empty_records()
    frame = pd.DataFrame(records)
    for column in SOURCE_RECORD_COLUMNS:
        if column not in frame:
            frame[column] = pd.NA
    frame = frame[SOURCE_RECORD_COLUMNS].copy()
    # Identical native snapshots are duplicate observations, not duplicate records.
    frame = frame.drop_duplicates(subset=["source_record_id"], keep="first")
    frame = _collapse_intra_source_duplicates(frame)
    return frame.reset_index(drop=True)


_INTRA_SOURCE_KEYS = [
    "source_system",
    "source_domain",
    "entity_grain",
    "normalized_name",
    "normalized_address",
    "district",
    "native_parent_id",
]


def _collapse_intra_source_duplicates(frame: pd.DataFrame) -> pd.DataFrame:
    """Collapse one source's repeated emissions of the same building.

    Centaline lists ~1,543 estates twice under native ids that differ only by a
    numeric prefix. Matching compares records across sources only, so it can
    never see these as duplicates; instead each pair reaches resolution as two
    identical candidates for one Midland record and the collision guard rejects
    all of them, taking the real match down with it.
    """
    # A name alone cannot identify a building: Midland files 56 different Yuen
    # Long estates' "Block 2" under that one name with no address at all. Require
    # an address or a parent estate before treating two rows as one building.
    named = frame["normalized_name"].fillna("").ne("") & (
        frame["normalized_address"].fillna("").ne("")
        | frame["native_parent_id"].fillna("").ne("")
    )
    if not named.any() or not (named & frame.duplicated(_INTRA_SOURCE_KEYS, keep=False)).any():
        return frame
    frame = frame.assign(
        observed_count=pd.to_numeric(frame["observed_count"], errors="coerce").fillna(0)
    )
    # Total the group's evidence before deduplication, so the surviving row can
    # carry it without the sum flattening the ordering that picks that row.
    frame["_group_observed"] = frame.groupby(_INTRA_SOURCE_KEYS, dropna=False)[
        "observed_count"
    ].transform("sum")
    # Keep the best-evidenced row per building; ties break on the id so the
    # choice never depends on input order.
    ordered = frame.sort_values(
        ["observed_count", "source_record_id"],
        ascending=[False, True],
        kind="stable",
    )
    is_named = named.reindex(ordered.index, fill_value=False)
    collapsed = ordered[is_named].drop_duplicates(
        subset=_INTRA_SOURCE_KEYS, keep="first"
    )
    collapsed = collapsed.assign(observed_count=collapsed["_group_observed"])
    return (
        pd.concat([collapsed, ordered[~is_named]])
        .drop(columns="_group_observed")
        .sort_values("source_record_id", kind="stable")
    )


def _record(  # noqa: PLR0913
    *,
    source_system: str,
    source_domain: str,
    source_dataset: str,
    entity_grain: str,
    native_id: Any,
    native_parent_id: Any = pd.NA,
    name_en: Any = pd.NA,
    name_zh: Any = pd.NA,
    complex_name_en: Any = pd.NA,
    complex_name_zh: Any = pd.NA,
    phase_name_en: Any = pd.NA,
    phase_name_zh: Any = pd.NA,
    building_name_en: Any = pd.NA,
    building_name_zh: Any = pd.NA,
    address_en: Any = pd.NA,
    address_zh: Any = pd.NA,
    region: Any = pd.NA,
    district: Any = pd.NA,
    subdistrict: Any = pd.NA,
    latitude: Any = pd.NA,
    longitude: Any = pd.NA,
    completion_values: Sequence[Any] = (),
    property_type: Any = pd.NA,
    source_url: Any = pd.NA,
    observed_count: int = 0,
    geo_confidence: Any = pd.NA,
    attribute_provenance: str = "catalog",
) -> dict[str, Any]:
    """Create one schema-conformant source record."""
    name_en = _clean_text(name_en)
    name_zh = _clean_text(name_zh)
    building_name_en = _clean_text(building_name_en)
    building_name_zh = _clean_text(building_name_zh)
    preferred_name = name_en if pd.notna(name_en) else building_name_en
    normalized_name = normalize_building_name(preferred_name)
    preferred_address = _clean_address(address_en)
    chinese_address = _clean_address(address_zh)
    source_record_id, resolved_native_id = _stable_id(
        source_system,
        source_domain,
        entity_grain,
        native_id,
        (
            source_system,
            source_domain,
            entity_grain,
            normalized_name,
            _normalized_address(preferred_address),
            _clean_text(native_parent_id),
        ),
    )
    year = _completion_year(*completion_values)
    return {
        "source_record_id": source_record_id,
        "source_system": source_system,
        "source_domain": source_domain,
        "source_dataset": source_dataset,
        "entity_grain": entity_grain,
        "native_id": resolved_native_id,
        "native_parent_id": _clean_text(native_parent_id),
        "name_en": name_en,
        "name_zh": name_zh,
        "complex_name_en": _clean_text(complex_name_en),
        "complex_name_zh": _clean_text(complex_name_zh),
        "phase_name_en": _clean_text(phase_name_en),
        "phase_name_zh": _clean_text(phase_name_zh),
        "building_name_en": building_name_en,
        "building_name_zh": building_name_zh,
        "normalized_name": normalized_name or pd.NA,
        "address_en": preferred_address,
        "address_zh": chinese_address,
        "normalized_address": (
            _normalized_address(preferred_address)
            or _normalized_address(chinese_address)
            or pd.NA
        ),
        "region": _clean_text(region),
        "district": _clean_text(district),
        "subdistrict": _clean_text(subdistrict),
        "latitude": latitude,
        "longitude": longitude,
        "completion_year": year,
        "age": _age(year),
        "property_type": _clean_text(property_type),
        "source_url": _clean_text(source_url),
        "observed_count": int(observed_count),
        "geo_confidence": geo_confidence,
        "attribute_provenance": attribute_provenance,
    }


def _transaction_candidates(  # noqa: PLR0913
    transactions: pd.DataFrame,
    *,
    source_system: str,
    source_domain: str,
    id_columns: Sequence[str],
    parent_columns: Sequence[str] = (),
    name_en_columns: Sequence[str],
    name_zh_columns: Sequence[str] = (),
    address_en_columns: Sequence[str] = (),
    address_zh_columns: Sequence[str] = (),
    street_number_columns: Sequence[str] = (),
    street_name_en_columns: Sequence[str] = (),
    street_name_zh_columns: Sequence[str] = (),
    region_columns: Sequence[str] = (),
    district_columns: Sequence[str] = (),
    subdistrict_columns: Sequence[str] = (),
    completion_columns: Sequence[str] = (),
    property_type_columns: Sequence[str] = (),
    exclude_native_ids: set[str] | None = None,
    dataset: str,
) -> list[dict[str, Any]]:
    """Aggregate transaction rows to building candidates; never returns transaction rows."""
    if transactions.empty:
        return []
    work = pd.DataFrame(
        {
            "native_id": _series(transactions, *id_columns),
            "parent_id": _series(transactions, *parent_columns),
            "name_en": _series(transactions, *name_en_columns),
            "name_zh": _series(transactions, *name_zh_columns),
            "address_en": _address_series(transactions, *address_en_columns),
            "address_zh": _address_series(transactions, *address_zh_columns),
            "street_number": _series(transactions, *street_number_columns),
            "street_name_en": _series(transactions, *street_name_en_columns),
            "street_name_zh": _series(transactions, *street_name_zh_columns),
            "region": _series(transactions, *region_columns),
            "district": _series(transactions, *district_columns),
            "subdistrict": _series(transactions, *subdistrict_columns),
            "completion": _series(transactions, *completion_columns),
            "property_type": _series(transactions, *property_type_columns),
        }
    )
    work["address_en"] = work["address_en"].where(
        work["address_en"].notna(),
        work.apply(
            lambda row: _compose_street_address(
                row["street_number"], row["street_name_en"]
            ),
            axis=1,
        ),
    )
    work["address_zh"] = work["address_zh"].where(
        work["address_zh"].notna(),
        work.apply(
            lambda row: _compose_street_address(
                row["street_number"], row["street_name_zh"]
            ),
            axis=1,
        ),
    )
    work["fallback_key"] = (
        work["name_en"].map(normalize_building_name)
        + "|"
        + work["parent_id"].fillna("").astype(str)
    )
    work["group_key"] = work["native_id"].fillna("fp:" + work["fallback_key"])
    work = work[work["group_key"].ne("fp:|")].copy()
    if exclude_native_ids:
        work = work[~work["native_id"].astype("string").isin(exclude_native_ids)]

    records: list[dict[str, Any]] = []
    for _, group in work.groupby("group_key", dropna=False, sort=True):
        # Modal values keep the record identical no matter what order the
        # transactions arrived in; incremental scrapes reorder rows every run.
        name_en = _mode(group["name_en"])
        name_zh = _mode(group["name_zh"])
        records.append(
            _record(
                source_system=source_system,
                source_domain=source_domain,
                source_dataset=dataset,
                entity_grain="building",
                native_id=_mode(group["native_id"]),
                native_parent_id=_mode(group["parent_id"]),
                name_en=name_en,
                name_zh=name_zh,
                building_name_en=name_en,
                building_name_zh=name_zh,
                address_en=_mode(group["address_en"]),
                address_zh=_mode(group["address_zh"]),
                region=_mode(group["region"]),
                district=_mode(group["district"]),
                subdistrict=_mode(group["subdistrict"]),
                completion_values=[_mode(group["completion"])],
                property_type=_mode(group["property_type"]),
                observed_count=len(group),
                geo_confidence=_modal_confidence(group, ("region", "district", "subdistrict")),
                attribute_provenance="transaction_aggregate",
            )
        )
    return records


def _mode(values: pd.Series) -> Any:
    cleaned = values.map(_clean_text).dropna()
    if cleaned.empty:
        return pd.NA
    counts = cleaned.value_counts()
    return sorted(counts[counts.eq(counts.max())].index.astype(str))[0]


def _modal_confidence(frame: pd.DataFrame, columns: Sequence[str]) -> Any:
    """Return modal geographical agreement, from zero to one."""
    scores: list[float] = []
    for column in columns:
        values = frame[column].map(_clean_text).dropna()
        if not values.empty:
            scores.append(float(values.value_counts().iloc[0]) / len(values))
    return round(sum(scores) / len(scores), 3) if scores else pd.NA


def _first_value(*values: Any) -> Any:
    """Return the first non-blank value without treating falsey IDs as blank."""
    for value in values:
        cleaned = _clean_text(value)
        if pd.notna(cleaned):
            return cleaned
    return pd.NA


def _first_address(*values: Any) -> Any:
    """Return the first non-coordinate address value."""
    for value in values:
        cleaned = _clean_address(value)
        if pd.notna(cleaned):
            return cleaned
    return pd.NA


def _aggregate_transaction_evidence(
    transactions: pd.DataFrame,
    *,
    id_columns: Sequence[str],
    fields: dict[str, Sequence[str]],
    geography_fields: Sequence[str],
) -> dict[str, dict[str, Any]]:
    """Aggregate linked transaction attributes once per native catalog ID."""
    if transactions.empty:
        return {}

    work = pd.DataFrame(
        {
            "native_id": _series(transactions, *id_columns),
            **{
                field_name: (
                    _address_series(transactions, *source_columns)
                    if field_name == "address"
                    else _series(transactions, *source_columns)
                )
                for field_name, source_columns in fields.items()
            },
        }
    )
    work = work[work["native_id"].notna()].copy()
    evidence_by_id: dict[str, dict[str, Any]] = {}
    for native_id, group in work.groupby("native_id", sort=True):
        evidence_by_id[str(native_id)] = {
            field_name: _mode(group[field_name]) for field_name in fields
        }
        evidence_by_id[str(native_id)]["observed_count"] = len(group)
        evidence_by_id[str(native_id)]["geo_confidence"] = _modal_confidence(
            group, geography_fields
        )
    return evidence_by_id


def _evidence_for(
    evidence_by_id: dict[str, dict[str, Any]], native_id: Any
) -> dict[str, Any]:
    cleaned_id = _clean_text(native_id)
    return evidence_by_id.get(str(cleaned_id), {}) if pd.notna(cleaned_id) else {}


def _street_address(evidence: dict[str, Any], *, chinese: bool = False) -> Any:
    """Build a modal street address when a catalog lacks an address."""
    street = evidence.get("street_name_zh" if chinese else "street_name_en", pd.NA)
    number = evidence.get("streetno", pd.NA)
    return _compose_street_address(number, street)


def _compose_street_address(number: Any, street: Any) -> Any:
    """Safely combine optional street components without fabricating text values."""
    return _clean_address(_first_value(
        " ".join(
            str(cleaned)
            for value in (number, street)
            if pd.notna(cleaned := _clean_text(value))
        ),
        street,
    ))


def _unique_estate_codes_by_name(estate_details: pd.DataFrame) -> dict[str, str]:
    """Return exact normalized estate-name mappings with one catalog code only."""
    if estate_details.empty:
        return {}
    catalog = pd.DataFrame(
        {
            "name": _series(estate_details, "Name"),
            "estate_code": _series(estate_details, "estate_code", "EstateCode"),
        }
    )
    catalog["normalized_name"] = catalog["name"].map(normalize_building_name)
    catalog = catalog[
        catalog["normalized_name"].notna()
        & catalog["normalized_name"].ne("")
        & catalog["estate_code"].notna()
    ]
    lookup: dict[str, str] = {}
    for normalized_name, group in catalog.groupby("normalized_name", sort=True):
        codes = sorted(group["estate_code"].astype(str).unique())
        if len(codes) == 1:
            lookup[str(normalized_name)] = codes[0]
    return lookup


def adapt_centaline_residential(
    estate_details: pd.DataFrame, transaction_base: pd.DataFrame
) -> pd.DataFrame:
    """Adapt Centaline residential estate/phase catalog and building candidates."""
    records: list[dict[str, Any]] = []
    candidate_transactions = transaction_base.copy()
    unique_estate_codes = _unique_estate_codes_by_name(estate_details)
    explicit_parent = _series(candidate_transactions, "estate_code", "estate_id")
    transaction_estate_name = _series(
        candidate_transactions,
        "estate_name",
        "estateName",
        "estate_name_en",
        "estateNameEn",
    )
    candidate_transactions["_resolved_estate_code"] = explicit_parent.where(
        explicit_parent.notna(),
        transaction_estate_name.map(normalize_building_name).map(unique_estate_codes),
    )
    evidence_by_id = _aggregate_transaction_evidence(
        transaction_base,
        id_columns=("estate_code", "estate_id"),
        fields={
            "name_en": ("estate_name", "Name"),
            "name_zh": ("estate_chinese_name", "propertyNameCn"),
            "address": ("street_address", "address", "estate_full_address"),
            "region": ("region", "estate_region"),
            "district": ("district", "estate_district"),
            "subdistrict": ("subdistrict", "estate_subdistrict"),
        },
        geography_fields=("region", "district", "subdistrict"),
    )
    for _, row in estate_details.iterrows():
        native_id = _first_value(row.get("estate_code"), row.get("EstateCode"))
        evidence = _evidence_for(evidence_by_id, native_id)
        records.append(
            _record(
                source_system="centaline",
                source_domain="residential",
                source_dataset="estate_details",
                entity_grain="estate_phase",
                native_id=native_id,
                name_en=_first_value(row.get("Name"), evidence.get("name_en")),
                name_zh=_first_value(
                    row.get("chinese_name"),
                    evidence.get("name_zh"),
                ),
                complex_name_en=_first_value(row.get("Name"), evidence.get("name_en")),
                complex_name_zh=_first_value(row.get("chinese_name"), evidence.get("name_zh")),
                phase_name_en=row.get("phase_name"),
                address_en=_first_address(
                    row.get("estate_detailed_address"),
                    row.get("Address"),
                    evidence.get("address"),
                ),
                region=_first_value(row.get("Region"), evidence.get("region")),
                district=_first_value(row.get("District"), evidence.get("district")),
                subdistrict=_first_value(row.get("Subdistrict"), evidence.get("subdistrict")),
                completion_values=[row.get("occupation_permit"), row.get("completion_year")],
                source_url=row.get("Link"),
                observed_count=evidence.get("observed_count", 0),
                geo_confidence=evidence.get("geo_confidence", pd.NA),
                attribute_provenance="estate_catalog;transaction_evidence" if evidence else "estate_catalog",
            )
        )
    records.extend(
        _transaction_candidates(
            candidate_transactions, source_system="centaline", source_domain="residential",
            id_columns=("building_code", "building_id"),
            parent_columns=("_resolved_estate_code",),
            name_en_columns=("building_name", "buildingName", "Tower", "Name"),
            name_zh_columns=("building_chinese_name", "estate_chinese_name"),
            address_en_columns=("street_address", "address", "address_en"),
            address_zh_columns=("address_zh",),
            region_columns=("region", "estate_region", "Region"),
            district_columns=("district", "estate_district", "District"),
            subdistrict_columns=("subdistrict", "Subdistrict"),
            completion_columns=("completion_year", "occupation_permit"),
            property_type_columns=("property_type",),
            dataset="transaction_building_candidates",
        )
    )
    return _finalize(records)


def adapt_midland_residential(estates: pd.DataFrame, transaction_base: pd.DataFrame) -> pd.DataFrame:
    """Adapt Midland residential estate catalog and building-level transaction candidates."""
    records: list[dict[str, Any]] = []
    evidence_by_id = _aggregate_transaction_evidence(
        transaction_base,
        id_columns=("estate_id", "id"),
        fields={
            "name_en": ("estate",),
            "name_zh": ("news_name",),
            "address": ("location", "address", "address_en"),
            "region": ("region_name_trans", "region_name"),
            # Midland's subregion is the 18-district level that Centaline calls
            # district; Midland's district is the locality Centaline calls
            # subdistrict. Mapping them by name instead of by level puts the
            # same building in two different blocks and it never matches.
            "district": ("subregion_name", "subregion"),
            "subdistrict": ("sm_district_name", "district_name", "district"),
        },
        geography_fields=("region", "district", "subdistrict"),
    )
    for _, row in estates.iterrows():
        native_id = _first_value(row.get("estate_id"), row.get("id"))
        evidence = _evidence_for(evidence_by_id, native_id)
        records.append(
            _record(
                source_system="midland", source_domain="residential",
                source_dataset="estate_catalog", entity_grain="estate_phase",
                native_id=native_id,
                native_parent_id=row.get("parent_estate_id"),
                name_en=_first_value(row.get("name"), row.get("estate"), evidence.get("name_en")),
                name_zh=_first_value(row.get("news_name"), evidence.get("name_zh")),
                complex_name_en=_first_value(row.get("parent_estate_name"), row.get("name"), row.get("estate")),
                phase_name_en=_first_value(row.get("name"), row.get("estate")),
                address_en=_first_address(row.get("location"), evidence.get("address")),
                region=_first_value(row.get("region_name"), evidence.get("region")),
                district=_first_value(row.get("subregion_name"), row.get("subregion"), evidence.get("district")),
                subdistrict=_first_value(
                    row.get("sm_district_name"), row.get("district_name"), row.get("district"),
                    evidence.get("subdistrict"),
                ),
                latitude=row.get("location_lat"), longitude=row.get("location_lon"),
                completion_values=[row.get("first_op_date")],
                source_url=row.get("url_desc"), observed_count=evidence.get("observed_count", 0),
                geo_confidence=evidence.get("geo_confidence", pd.NA),
                attribute_provenance="estate_catalog;transaction_evidence" if evidence else "estate_catalog",
            )
        )
    records.extend(
        _transaction_candidates(
            transaction_base, source_system="midland", source_domain="residential",
            id_columns=("building_id",), parent_columns=("estate_id", "parent_estate_id"),
            name_en_columns=("building", "building_name"),
            name_zh_columns=("news_name",),
            address_en_columns=("location", "address", "address_en"),
            address_zh_columns=("address_zh",),
            region_columns=("region_name_trans", "region_name"),
            district_columns=("subregion_name", "subregion"),
            subdistrict_columns=("sm_district_name", "district_name", "district", "subdistrict"),
            completion_columns=("building_first_op_date", "completion_year"),
            property_type_columns=("property_type",), dataset="transaction_building_candidates",
        )
    )
    return _finalize(records)


def adapt_centaline_commercial(
    building_details: pd.DataFrame, transaction_base: pd.DataFrame
) -> pd.DataFrame:
    """Adapt Centaline OIR catalog buildings and unmatched transaction aliases."""
    records: list[dict[str, Any]] = []
    catalog_ids: set[str] = set()
    evidence_by_id = _aggregate_transaction_evidence(
        transaction_base,
        id_columns=("propertyId", "property_id"),
        fields={
            "name_en": ("propertyNameEn",),
            "name_zh": ("propertyNameCn",),
            "address": ("full_address", "address", "address_en"),
            "region": ("zoneEn", "zone"),
            "district": ("districtNameEn", "district"),
            "property_type": ("propertyUsageDisplayName", "property_type"),
        },
        geography_fields=("region", "district"),
    )
    for _, row in building_details.iterrows():
        native_id = _first_value(row.get("property_id"), row.get("propertyID"), row.get("propertyId"))
        cleaned_id = _clean_text(native_id)
        if pd.notna(cleaned_id):
            catalog_ids.add(str(cleaned_id))
        evidence = _evidence_for(evidence_by_id, native_id)
        records.append(
            _record(
                source_system="centaline", source_domain="commercial",
                source_dataset="building_details", entity_grain="building", native_id=native_id,
                name_en=_first_value(row.get("building_name"), row.get("buildingNameEn"), evidence.get("name_en")),
                name_zh=_first_value(row.get("building_name_zh"), row.get("buildingNameCn"), evidence.get("name_zh")),
                building_name_en=_first_value(row.get("building_name"), row.get("buildingNameEn"), evidence.get("name_en")),
                building_name_zh=_first_value(row.get("building_name_zh"), row.get("buildingNameCn"), evidence.get("name_zh")),
                address_en=_first_address(
                    row.get("full_address"),
                    row.get("address"),
                    evidence.get("address"),
                ),
                region=_first_value(row.get("zone"), row.get("zoneEn"), evidence.get("region")),
                district=_first_value(row.get("district"), row.get("districtNameEn"), evidence.get("district")),
                completion_values=[row.get("completion_year"), row.get("Year of Completion")],
                property_type=_first_value(row.get("property_type"), row.get("usage"), evidence.get("property_type")),
                source_url=row.get("source_url"), observed_count=evidence.get("observed_count", 0),
                geo_confidence=evidence.get("geo_confidence", pd.NA),
                attribute_provenance="building_catalog;transaction_evidence" if evidence else "building_catalog",
            )
        )
    records.extend(
        _transaction_candidates(
            transaction_base, source_system="centaline", source_domain="commercial",
            id_columns=("propertyId", "property_id"),
            name_en_columns=("propertyNameEn", "building_name", "matched_building_name"),
            name_zh_columns=("propertyNameCn",), region_columns=("zoneEn", "zone"),
            address_en_columns=("full_address", "address", "address_en"),
            address_zh_columns=("address_zh",),
            district_columns=("districtNameEn", "district"), completion_columns=("completion_year",),
            property_type_columns=("propertyUsageDisplayName", "property_type"),
            exclude_native_ids=catalog_ids, dataset="transaction_building_aliases",
        )
    )
    return _finalize(records)


def adapt_midland_commercial(
    building_details: pd.DataFrame, transaction_base: pd.DataFrame
) -> pd.DataFrame:
    """Adapt Midland ICI buildings, enriching catalog geography from transactions."""
    records: list[dict[str, Any]] = []
    catalog_ids: set[str] = set()
    evidence_by_id = _aggregate_transaction_evidence(
        transaction_base,
        id_columns=("building_id", "id"),
        fields={
            "name_en": ("eng_name",),
            "name_zh": ("chi_name",),
            "streetno": ("streetno", "street_no"),
            "street_name_en": ("street_name_en",),
            "street_name_zh": ("street_name_zh",),
            "region": ("zoneEn", "zone"),
            "district": ("dist_name_en", "district"),
        },
        geography_fields=("region", "district"),
    )
    for _, row in building_details.iterrows():
        native_id = _first_value(row.get("building_id"), row.get("id"))
        cleaned_id = _clean_text(native_id)
        if pd.notna(cleaned_id):
            catalog_ids.add(str(cleaned_id))
        evidence = _evidence_for(evidence_by_id, native_id)
        records.append(
            _record(
                source_system="midland", source_domain="commercial",
                source_dataset="building_details", entity_grain="building", native_id=native_id,
                name_en=_first_value(row.get("Building Name"), row.get("name_en"), evidence.get("name_en")),
                name_zh=_first_value(row.get("Building Name ZH"), row.get("name_zh"), evidence.get("name_zh")),
                building_name_en=_first_value(row.get("Building Name"), row.get("name_en"), evidence.get("name_en")),
                building_name_zh=_first_value(row.get("Building Name ZH"), row.get("name_zh"), evidence.get("name_zh")),
                address_en=_first_address(
                    row.get("Address"),
                    row.get("full_address"),
                    row.get("address"),
                    _street_address(evidence),
                ),
                address_zh=_first_address(
                    row.get("Address ZH"),
                    row.get("address_zh"),
                    _street_address(evidence, chinese=True),
                ),
                region=_first_value(row.get("zoneEn"), row.get("zone"), evidence.get("region")),
                district=_first_value(row.get("dist_name_en"), row.get("district"), evidence.get("district")),
                completion_values=[row.get("Completion"), row.get("Completion Date")],
                property_type=row.get("Type"), source_url=row.get("URL"),
                observed_count=evidence.get("observed_count", 0),
                geo_confidence=evidence.get("geo_confidence", pd.NA),
                attribute_provenance="building_catalog;transaction_geography" if evidence else "building_catalog",
            )
        )
    records.extend(
        _transaction_candidates(
            transaction_base, source_system="midland", source_domain="commercial",
            id_columns=("building_id", "id"), name_en_columns=("eng_name", "Building Name"),
            name_zh_columns=("chi_name",), region_columns=("zoneEn", "zone"),
            address_en_columns=("address_en", "full_address", "address"),
            address_zh_columns=("address_zh",),
            street_number_columns=("streetno", "street_no"),
            street_name_en_columns=("street_name_en",),
            street_name_zh_columns=("street_name_zh",),
            district_columns=("dist_name_en", "district"), completion_columns=("Completion",),
            property_type_columns=("ics_type", "Type"), exclude_native_ids=catalog_ids,
            dataset="transaction_building_aliases",
        )
    )
    return _finalize(records)


def combine_source_records(
    centaline_residential: pd.DataFrame,
    midland_residential: pd.DataFrame,
    centaline_commercial: pd.DataFrame,
    midland_commercial: pd.DataFrame,
) -> pd.DataFrame:
    """Combine four adapter outputs while preserving the ordered source contract."""
    frames = (centaline_residential, midland_residential, centaline_commercial, midland_commercial)
    combined = pd.concat([frame.reindex(columns=SOURCE_RECORD_COLUMNS) for frame in frames], ignore_index=True)
    if combined["source_record_id"].duplicated().any():
        duplicates = combined.loc[combined["source_record_id"].duplicated(), "source_record_id"].tolist()
        raise ValueError(f"source_record_id values must be unique: {duplicates}")
    return combined[SOURCE_RECORD_COLUMNS]
