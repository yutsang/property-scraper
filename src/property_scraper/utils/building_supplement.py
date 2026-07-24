from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd


logger = logging.getLogger(__name__)
MANUAL_RECORD_SOURCE = "manual_workbook"

SUPPLEMENTAL_MASTER_COLUMNS = [
    "domain",
    "source_system",
    "source_join_key",
    "source_name_raw",
    "normalized_name",
    "district_code",
    "district_name_en",
    "zone_en",
    "candidate_building_name",
    "candidate_source",
    "review_status",
    "review_notes",
    "canonical_building_name",
    "address",
    "completion_year",
    "management_company",
    "url",
    "match_origin",
    "record_source",
]

REVIEW_QUEUE_COLUMNS = [
    *SUPPLEMENTAL_MASTER_COLUMNS,
    "transaction_count",
]

MEGA_WORKBOOK_TABS = {
    "source_a_res": "source_a_res",
    "source_a_commercial": "source_a_commercial",
    "source_b_res": "source_b_res",
    "source_b_commercial": "source_b_commercial",
}

MEGA_BASE_COLUMNS = [
    "row_kind",
    "domain",
    "source_system",
    "source_join_key",
    "source_name_raw",
    "normalized_name",
    "district_code",
    "district_name_en",
    "zone_en",
    "occurrence_count",
    "candidate_building_name",
    "candidate_source",
    "native_address",
    "native_completion_year",
    "native_management_company",
    "native_url",
    "native_detail_source",
    "manual_canonical_name",
    "manual_address",
    "manual_completion_year",
    "manual_management_company",
    "manual_url",
    "manual_notes",
    "manual_include",
]

MEGA_SHEET_COLUMNS = {
    "source_a_res": [
        *MEGA_BASE_COLUMNS,
        "native_developer",
        "native_blocks",
        "native_link",
    ],
    "source_a_commercial": [
        *MEGA_BASE_COLUMNS,
        "native_grade",
        "native_usage",
        "native_property_type",
        "native_developers",
    ],
    "source_b_res": [
        *MEGA_BASE_COLUMNS,
        "native_developer",
        "native_total_unit_count",
        "native_total_block_count",
    ],
    "source_b_commercial": [
        *MEGA_BASE_COLUMNS,
        "native_grade",
        "native_type",
        "native_title",
        "native_total_area",
    ],
}

AUDIT_SUMMARY_COLUMNS = [
    "domain",
    "source_system",
    "metric",
    "status",
    "row_count",
]

_BLANK_VALUES = {"", "NONE", "NAN", "NULL", "NA", "<NA>"}
_TOKEN_REPLACEMENTS = {
    "BLDG": "BUILDING",
    "BLDGS": "BUILDINGS",
    "CTR": "CENTRE",
    "CTRS": "CENTRES",
    "PLZ": "PLAZA",
    "HSE": "HOUSE",
    "TWRS": "TOWERS",
    "TWR": "TOWER",
    "INDL": "INDUSTRIAL",
}
_EXCEL_ILLEGAL_CHAR_RE = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")


def normalize_building_name(value: Any) -> str:
    """Normalize building names for deterministic cross-source matching."""
    if value is None or pd.isna(value):
        return ""

    text = str(value).upper().strip()
    if not text:
        return ""

    text = text.replace("&", " AND ")
    text = re.sub(r"[’']", "", text)
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    tokens = [_TOKEN_REPLACEMENTS.get(token, token) for token in text.split()]
    return " ".join(tokens)


def load_excel_supplemental_master(
    workbook_path: Path,
    *,
    domain: Optional[str] = None,
    source_system: Optional[str] = None,
) -> pd.DataFrame:
    """Load approved manual mappings from the Excel workbook."""
    if not workbook_path.exists():
        return _empty_supplemental_master()

    combined = extract_approved_rows_from_mega_workbook(
        load_existing_mega_workbook_tabs(workbook_path)
    )

    if combined.empty:
        return combined

    combined = _ensure_columns(combined, SUPPLEMENTAL_MASTER_COLUMNS)
    combined["domain"] = combined["domain"].fillna("commercial").astype("string")
    combined["source_system"] = combined["source_system"].astype("string")
    combined["source_join_key"] = combined["source_join_key"].map(_clean_key)
    combined["source_name_raw"] = combined["source_name_raw"].astype("string")
    combined["normalized_name"] = combined["normalized_name"].where(
        combined["normalized_name"].map(bool),
        combined["canonical_building_name"].map(normalize_building_name),
    )
    combined["district_name_en"] = combined["district_name_en"].map(_clean_text)
    combined["zone_en"] = combined["zone_en"].map(_clean_text)
    combined["review_status"] = "approved"
    combined["match_origin"] = combined["match_origin"].fillna("manual").astype("string")
    combined["record_source"] = combined["record_source"].fillna("excel_manual").astype("string")
    combined = _dedupe_master_rows(combined)

    if domain:
        combined = combined[combined["domain"] == domain]
    if source_system:
        combined = combined[combined["source_system"] == source_system]
    return combined.reset_index(drop=True)


def _derive_source_b_commercial_building_geo(
    source_b_commercial_base: pd.DataFrame,
) -> pd.DataFrame:
    """Recover per-building district/zone for Source B ICI from transaction data.

    midland_ici_building_details.parquet (the native building-level scrape) never
    captured district/zone, even though every transaction row already carries
    dist_code/dist_name_en. This aggregates the transaction-level values (most
    common per building_id) so cross-source building matching keys line up with
    Source A commercial, which already populates district_name_en/zone_en natively.
    """
    if source_b_commercial_base.empty or "building_id" not in source_b_commercial_base.columns:
        return pd.DataFrame(columns=["building_id", "district_name_en", "zone_en"])

    # Reuses the authoritative Source B district->zone mapping already maintained
    # for the main data_process pipeline, so the two stay in sync.
    from ..pipelines.data_process.nodes import _source_b_zone_from_dist_code

    working = source_b_commercial_base[source_b_commercial_base["building_id"].notna()].copy()
    if working.empty:
        return pd.DataFrame(columns=["building_id", "district_name_en", "zone_en"])

    working["_zone_en"] = working.apply(
        lambda row: _source_b_zone_from_dist_code(
            row.get("dist_code"), row.get("dist_name_en")
        ),
        axis=1,
    )

    def _mode_or_na(series: pd.Series) -> Any:
        cleaned = series.dropna()
        cleaned = cleaned[cleaned.astype(str).str.strip() != ""]
        if cleaned.empty:
            return pd.NA
        return cleaned.mode().iloc[0]

    aggregated = (
        working.groupby("building_id")
        .agg(
            district_name_en=("dist_name_en", _mode_or_na),
            zone_en=("_zone_en", _mode_or_na),
        )
        .reset_index()
    )
    return aggregated


def _enrich_source_b_commercial_buildings_with_geo(
    source_b_commercial_buildings: pd.DataFrame,
    source_b_commercial_base: pd.DataFrame,
) -> pd.DataFrame:
    """Attach district_name_en/zone_en to the Source B ICI native buildings frame."""
    if source_b_commercial_buildings.empty or "id" not in source_b_commercial_buildings.columns:
        return source_b_commercial_buildings

    geo = _derive_source_b_commercial_building_geo(source_b_commercial_base)
    if geo.empty:
        return source_b_commercial_buildings

    return source_b_commercial_buildings.merge(
        geo, left_on="id", right_on="building_id", how="left"
    ).drop(columns=["building_id"], errors="ignore")


def build_mega_building_workbook(
    *,
    source_a_commercial: pd.DataFrame,
    source_b_commercial_base: pd.DataFrame,
    source_b_commercial_primary: pd.DataFrame,
    source_a_res: pd.DataFrame,
    source_b_res: pd.DataFrame,
    source_a_commercial_buildings: pd.DataFrame,
    source_b_commercial_buildings: pd.DataFrame,
    source_a_res_buildings: pd.DataFrame,
    source_b_res_buildings: pd.DataFrame,
    workbook_path: Path,
    include_limit: int,
    source_c_frames: Iterable[pd.DataFrame],
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame, pd.DataFrame]:
    """Build one combined workbook tab per source and extract approved manual rows."""
    candidate_lookup = _build_candidate_lookup(
        source_b_commercial_primary=source_b_commercial_primary,
        source_c_frames=source_c_frames,
    )
    existing_tabs = load_existing_mega_workbook_tabs(workbook_path)

    # Source B ICI's native building-details frame never captured district/zone;
    # recover it from the transaction-level data so it can be used for building
    # matching/consolidation just like Source A commercial's native district/zone.
    source_b_commercial_buildings = _enrich_source_b_commercial_buildings_with_geo(
        source_b_commercial_buildings, source_b_commercial_base
    )

    fresh_candidates = {
        "source_a_res": _build_source_a_res_candidate_rows(source_a_res),
        "source_a_commercial": _build_mega_source_a_commercial_candidates(
            source_a_commercial,
            candidate_lookup,
        ),
        "source_b_res": _build_source_b_res_candidate_rows(source_b_res),
        "source_b_commercial": _build_mega_source_b_commercial_candidates(
            source_b_commercial_base,
            candidate_lookup,
        ),
    }
    native_rows = {
        "source_a_res": _build_source_a_res_native_rows(source_a_res_buildings),
        "source_a_commercial": _build_source_a_commercial_native_rows(source_a_commercial_buildings),
        "source_b_res": _build_source_b_res_native_rows(source_b_res_buildings),
        "source_b_commercial": _build_source_b_commercial_native_rows(source_b_commercial_buildings),
    }

    workbook_sheets: dict[str, pd.DataFrame] = {}
    for source_system in MEGA_WORKBOOK_TABS:
        merged_candidates = _merge_mega_candidate_rows(
            fresh_candidates=fresh_candidates[source_system],
            existing_sheet=existing_tabs.get(source_system, pd.DataFrame()),
            source_system=source_system,
            include_limit=include_limit,
        )
        combined_rows = [
            record
            for frame in (merged_candidates, native_rows[source_system])
            if not frame.empty
            for record in _ensure_columns(frame.copy(), MEGA_SHEET_COLUMNS[source_system]).to_dict("records")
        ]
        workbook_sheets[source_system] = _sort_mega_sheet_rows(
            pd.DataFrame.from_records(
                combined_rows,
                columns=MEGA_SHEET_COLUMNS[source_system],
            )
            if combined_rows
            else pd.DataFrame(columns=MEGA_SHEET_COLUMNS[source_system]),
            source_system=source_system,
        )

    supplement_cache = extract_approved_rows_from_mega_workbook(workbook_sheets)
    consolidated_commercial = build_consolidated_commercial_master(
        source_a_buildings=source_a_commercial_buildings,
        source_b_buildings=source_b_commercial_buildings,
        manual_master=supplement_cache[supplement_cache["domain"] == "commercial"].copy()
        if not supplement_cache.empty
        else pd.DataFrame(columns=SUPPLEMENTAL_MASTER_COLUMNS),
    )
    return workbook_sheets, supplement_cache, consolidated_commercial


def load_existing_mega_workbook_tabs(workbook_path: Path) -> dict[str, pd.DataFrame]:
    return {
        source_system: _read_excel_sheet(
            workbook_path,
            sheet_name,
            MEGA_SHEET_COLUMNS[source_system],
        )
        for source_system, sheet_name in MEGA_WORKBOOK_TABS.items()
    }


def write_mega_building_workbook(
    workbook_path: Path,
    workbook_sheets: dict[str, pd.DataFrame],
) -> None:
    workbook_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
        for source_system, sheet_name in MEGA_WORKBOOK_TABS.items():
            frame = workbook_sheets.get(
                source_system,
                pd.DataFrame(columns=MEGA_SHEET_COLUMNS[source_system]),
            )
            _sanitize_for_excel(
                _ensure_columns(frame.copy(), MEGA_SHEET_COLUMNS[source_system])
            ).to_excel(
                writer,
                sheet_name=sheet_name,
                index=False,
            )


def extract_approved_rows_from_mega_workbook(
    workbook_sheets: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    approved_frames: list[pd.DataFrame] = []
    for source_system, sheet_name in MEGA_WORKBOOK_TABS.items():
        frame = workbook_sheets.get(source_system, pd.DataFrame())
        if frame.empty:
            continue
        prepared = _ensure_columns(frame.copy(), MEGA_SHEET_COLUMNS[source_system])
        include_mask = prepared["manual_include"].map(_is_truthy_value)
        canonical_mask = prepared["manual_canonical_name"].map(_clean_text).notna()
        approved = prepared[include_mask & canonical_mask].copy()
        if approved.empty:
            continue
        approved["row_kind"] = "manual_approved"
        approved_frames.append(
            pd.DataFrame(
                {
                    "domain": approved["domain"].fillna(
                        "commercial" if source_system in {"source_a_commercial", "source_b_commercial"} else "residential"
                    ),
                    "source_system": source_system,
                    "source_join_key": approved["source_join_key"].map(_clean_key),
                    "source_name_raw": approved["source_name_raw"],
                    "normalized_name": approved["normalized_name"].where(
                        approved["normalized_name"].map(bool),
                        approved["manual_canonical_name"].map(normalize_building_name),
                    ),
                    "district_code": approved["district_code"],
                    "district_name_en": approved["district_name_en"],
                    "zone_en": approved["zone_en"],
                    "candidate_building_name": approved["candidate_building_name"],
                    "candidate_source": approved["candidate_source"],
                    "review_status": "approved",
                    "review_notes": approved["manual_notes"],
                    "canonical_building_name": approved["manual_canonical_name"],
                    "address": approved["manual_address"],
                    "completion_year": approved["manual_completion_year"],
                    "management_company": approved["manual_management_company"],
                    "url": approved["manual_url"],
                    "match_origin": "manual",
                    "record_source": "mega_workbook",
                }
            )
        )
    if not approved_frames:
        return _empty_supplemental_master()
    return _dedupe_master_rows(
        _ensure_columns(pd.concat(approved_frames, ignore_index=True), SUPPLEMENTAL_MASTER_COLUMNS)
    )


def merge_manual_rows_into_source_buildings(
    *,
    approved_master: pd.DataFrame,
    source_a_commercial_buildings: pd.DataFrame,
    source_b_commercial_buildings: pd.DataFrame,
    source_a_res_buildings: pd.DataFrame,
    source_b_res_buildings: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """Rewrite source building tables so workbook-approved manual rows become authoritative."""
    prepared_master = _ensure_columns(approved_master.copy(), SUPPLEMENTAL_MASTER_COLUMNS)
    return {
        "source_a_commercial": _merge_manual_rows_for_source(
            base_frame=source_a_commercial_buildings,
            manual_rows=_build_source_a_commercial_manual_rows(prepared_master),
            row_key_builder=_source_a_commercial_building_key,
        ),
        "source_b_commercial": _merge_manual_rows_for_source(
            base_frame=source_b_commercial_buildings,
            manual_rows=_build_source_b_commercial_manual_rows(prepared_master),
            row_key_builder=_source_b_commercial_building_key,
        ),
        "source_a_res": _merge_manual_rows_for_source(
            base_frame=source_a_res_buildings,
            manual_rows=_build_source_a_res_manual_rows(prepared_master),
            row_key_builder=_source_a_res_building_key,
        ),
        "source_b_res": _merge_manual_rows_for_source(
            base_frame=source_b_res_buildings,
            manual_rows=_build_source_b_res_manual_rows(prepared_master),
            row_key_builder=_source_b_res_building_key,
        ),
    }


def build_consolidated_commercial_master(
    *,
    source_a_buildings: pd.DataFrame,
    source_b_buildings: pd.DataFrame,
    manual_master: pd.DataFrame,
) -> pd.DataFrame:
    """Build a future-facing combined commercial building dictionary."""
    native_rows: list[dict[str, Any]] = []

    if not source_a_buildings.empty:
        for row in source_a_buildings.itertuples(index=False):
            building_name = getattr(row, "building_name", None)
            normalized = normalize_building_name(building_name)
            if not normalized:
                continue
            native_rows.append(
                {
                    "canonical_building_name": building_name,
                    "normalized_name": normalized,
                    "district_name_en": getattr(row, "district", None),
                    "zone_en": getattr(row, "zone", None),
                    "source_system": "source_a_commercial",
                    "address": getattr(row, "full_address", None),
                    "completion_year": getattr(row, "completion_year", None),
                    "management_company": getattr(row, "management_company", None),
                    "grade": getattr(row, "grade", None),
                    "provenance": "native",
                }
            )

    if not source_b_buildings.empty:
        for _, row in source_b_buildings.iterrows():
            building_name = row.get("Building Name")
            normalized = normalize_building_name(building_name)
            if not normalized:
                continue
            native_rows.append(
                {
                    "canonical_building_name": building_name,
                    "normalized_name": normalized,
                    # district_name_en/zone_en come from _enrich_source_b_commercial_buildings_with_geo
                    # (build_mega_building_workbook), which backfills them from transaction data since
                    # the native building scrape never captured district/zone directly.
                    "district_name_en": row.get("district_name_en"),
                    "zone_en": row.get("zone_en"),
                    "source_system": "source_b_commercial",
                    "address": None,
                    "completion_year": row.get("Completion"),
                    "management_company": row.get("Management Company"),
                    "grade": row.get("Grade"),
                    "provenance": "native",
                }
            )

    native_df = pd.DataFrame(native_rows)
    manual_df = pd.DataFrame()
    if not manual_master.empty:
        manual_df = pd.DataFrame(
            {
                "canonical_building_name": manual_master["canonical_building_name"],
                "normalized_name": manual_master["normalized_name"],
                "district_name_en": manual_master["district_name_en"],
                "zone_en": manual_master["zone_en"],
                "source_system": manual_master["source_system"],
                "address": manual_master["address"],
                "completion_year": manual_master["completion_year"],
                "management_company": manual_master["management_company"],
                "provenance": "manual",
            }
        )

    combined = pd.concat(
        [frame for frame in (native_df, manual_df) if not frame.empty],
        ignore_index=True,
    ) if (not native_df.empty or not manual_df.empty) else pd.DataFrame()
    if combined.empty:
        return pd.DataFrame(
            columns=[
                "canonical_building_name",
                "normalized_name",
                "district_name_en",
                "zone_en",
                "source_systems",
                "native_source_count",
                "manual_source_count",
                "has_manual_record",
                "preferred_completion_year",
                "preferred_address",
                "preferred_management_company",
                "preferred_grade",
                "provenance",
            ]
        )

    grouped_rows = []
    for _, group in combined.groupby(
        ["normalized_name", "district_name_en", "zone_en"], dropna=False
    ):
        manual_group = group[group["provenance"] == "manual"]
        preferred_group = manual_group if not manual_group.empty else group
        preferred_name = _first_non_blank_value(preferred_group["canonical_building_name"])
        if preferred_name is None:
            continue
        grouped_rows.append(
            {
                "canonical_building_name": preferred_name,
                "normalized_name": group["normalized_name"].iloc[0],
                "district_name_en": group["district_name_en"].iloc[0],
                "zone_en": group["zone_en"].iloc[0],
                "source_systems": ",".join(sorted(set(group["source_system"].dropna().astype(str)))),
                "native_source_count": int((group["provenance"] == "native").sum()),
                "manual_source_count": int((group["provenance"] == "manual").sum()),
                "has_manual_record": bool((group["provenance"] == "manual").any()),
                "preferred_completion_year": preferred_group["completion_year"]
                .dropna()
                .astype(str)
                .iloc[0]
                if preferred_group["completion_year"].dropna().any()
                else pd.NA,
                "preferred_address": preferred_group["address"]
                .dropna()
                .astype(str)
                .iloc[0]
                if preferred_group["address"].dropna().any()
                else pd.NA,
                "preferred_management_company": preferred_group["management_company"]
                .dropna()
                .astype(str)
                .iloc[0]
                if preferred_group["management_company"].dropna().any()
                else pd.NA,
                "preferred_grade": preferred_group["grade"]
                .dropna()
                .astype(str)
                .iloc[0]
                if "grade" in preferred_group.columns and preferred_group["grade"].dropna().any()
                else pd.NA,
                "provenance": "manual" if (group["provenance"] == "manual").any() else "native",
            }
        )

    if not grouped_rows:
        return pd.DataFrame(
            columns=[
                "canonical_building_name",
                "normalized_name",
                "district_name_en",
                "zone_en",
                "source_systems",
                "native_source_count",
                "manual_source_count",
                "has_manual_record",
                "preferred_completion_year",
                "preferred_address",
                "preferred_management_company",
                "preferred_grade",
                "provenance",
            ]
        )

    return pd.DataFrame(grouped_rows).sort_values(
        by=["has_manual_record", "canonical_building_name"],
        ascending=[False, True],
    ).reset_index(drop=True)


def load_supplemental_building_master(
    params: dict[str, Any],
    *,
    domain: Optional[str] = None,
    source_system: Optional[str] = None,
) -> pd.DataFrame:
    """Load the optional manually reviewed supplemental master."""
    config = params.get("building_supplement", {})
    if not config.get("enabled", False):
        return _empty_supplemental_master()

    raw_path = config.get("master_file") or config.get("workbook_file")
    if not raw_path:
        return _empty_supplemental_master()

    path = Path(raw_path)
    if not path.exists():
        logger.info("Supplemental building master file not found: %s", path)
        return _empty_supplemental_master()

    if path.suffix.lower() in {".xlsx", ".xlsm", ".xls"}:
        return load_excel_supplemental_master(
            path,
            domain=domain,
            source_system=source_system,
        )

    if path.suffix.lower() == ".csv":
        master = pd.read_csv(path)
    else:
        master = pd.read_parquet(path)

    if master.empty:
        return _empty_supplemental_master()

    for column in SUPPLEMENTAL_MASTER_COLUMNS:
        if column not in master.columns:
            master[column] = pd.NA

    master["domain"] = master["domain"].astype("string")
    master["source_system"] = master["source_system"].astype("string")
    master["review_status"] = master["review_status"].astype("string").str.lower()
    master["normalized_name"] = master["normalized_name"].where(
        master["normalized_name"].notna(),
        master["source_name_raw"].map(normalize_building_name),
    )
    master["source_join_key"] = master["source_join_key"].map(_clean_key)
    master["district_name_en"] = master["district_name_en"].map(_clean_text)
    master["zone_en"] = master["zone_en"].map(_clean_text)

    approved = master[master["review_status"] == "approved"].copy()
    if domain:
        approved = approved[approved["domain"] == domain]
    if source_system:
        approved = approved[approved["source_system"] == source_system]

    return approved.reset_index(drop=True)


def apply_supplemental_matches(
    *,
    unmatched_df: pd.DataFrame,
    supplemental_master: pd.DataFrame,
    source_system: str,
    domain: str,
    join_key_col: Optional[str],
    source_name_col: str,
    district_name_col: Optional[str],
    output_match_method_col: str,
    zone_col: Optional[str] = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply reviewed supplemental mappings to previously unmatched rows."""
    if unmatched_df.empty:
        return unmatched_df.copy(), unmatched_df.copy()

    if supplemental_master.empty:
        return pd.DataFrame(), unmatched_df.copy()

    master = supplemental_master.copy()
    master = master[
        (master["source_system"] == source_system) & (master["domain"] == domain)
    ].copy()
    if master.empty:
        return pd.DataFrame(), unmatched_df.copy()

    working = unmatched_df.copy().reset_index(drop=False).rename(
        columns={"index": "_supp_original_index"}
    )
    matched_parts: list[pd.DataFrame] = []

    if join_key_col and join_key_col in working.columns:
        keyed_master = master[master["source_join_key"].map(bool)].copy()
        if not keyed_master.empty:
            working["_supp_join_key"] = working[join_key_col].map(_clean_key)
            keyed_master["_master_join_key"] = keyed_master["source_join_key"].map(
                _clean_key
            )
            exact = _merge_first_match(
                working,
                keyed_master,
                left_on=["_supp_join_key"],
                right_on=["_master_join_key"],
                method="supplement_exact_key",
                output_match_method_col=output_match_method_col,
            )
            if not exact.empty:
                matched_parts.append(exact)
                working = working[
                    ~working["_supp_original_index"].isin(exact["_supp_original_index"])
                ].copy()

    if not working.empty:
        named_master = master[master["normalized_name"].map(bool)].copy()
        if not named_master.empty and source_name_col in working.columns:
            working["_supp_normalized_name"] = working[source_name_col].map(
                normalize_building_name
            )
            named_master["_master_normalized_name"] = named_master["normalized_name"].map(
                normalize_building_name
            )

            left_on = ["_supp_normalized_name"]
            right_on = ["_master_normalized_name"]
            if district_name_col and district_name_col in working.columns:
                working["_supp_district_name_en"] = working[district_name_col].map(
                    _clean_text
                )
                named_master["_master_district_name_en"] = named_master[
                    "district_name_en"
                ].map(_clean_text)
                district_ready = named_master["_master_district_name_en"].map(bool)
                if district_ready.any():
                    left_on.append("_supp_district_name_en")
                    right_on.append("_master_district_name_en")

            if zone_col and zone_col in working.columns:
                working["_supp_zone_en"] = working[zone_col].map(_clean_text)
                named_master["_master_zone_en"] = named_master["zone_en"].map(_clean_text)
                zone_ready = named_master["_master_zone_en"].map(bool)
                if zone_ready.any():
                    left_on.append("_supp_zone_en")
                    right_on.append("_master_zone_en")

            name_matched = _merge_first_match(
                working,
                named_master,
                left_on=left_on,
                right_on=right_on,
                method="supplement_reviewed_name",
                output_match_method_col=output_match_method_col,
            )
            if not name_matched.empty:
                matched_parts.append(name_matched)
                working = working[
                    ~working["_supp_original_index"].isin(
                        name_matched["_supp_original_index"]
                    )
                ].copy()

    if not matched_parts:
        return pd.DataFrame(), unmatched_df.copy()

    matched = pd.concat(matched_parts, ignore_index=True)
    remaining = (
        working.drop(
            columns=[
                col
                for col in working.columns
                if col.startswith("_supp_") and col != "_supp_original_index"
            ],
            errors="ignore",
        )
        .drop(columns=["_supp_original_index"], errors="ignore")
        .reset_index(drop=True)
    )

    matched = matched.drop(columns=["_supp_original_index"], errors="ignore")
    return matched.reset_index(drop=True), remaining


def build_supplemental_review_queue(
    *,
    source_a_commercial: pd.DataFrame,
    source_b_commercial_base: pd.DataFrame,
    source_b_commercial_primary: pd.DataFrame,
    source_a_res: Optional[pd.DataFrame] = None,
    source_b_res: Optional[pd.DataFrame] = None,
    source_c_frames: Optional[Iterable[pd.DataFrame]] = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build a prioritized manual review queue and a lightweight audit summary."""
    candidate_lookup = _build_candidate_lookup(
        source_b_commercial_primary=source_b_commercial_primary,
        source_c_frames=source_c_frames or [],
    )
    queue_frames = [
        _build_source_a_queue(source_a_commercial, candidate_lookup),
        _build_source_b_queue(source_b_commercial_base, candidate_lookup),
    ]
    non_empty_queue_frames = [frame for frame in queue_frames if not frame.empty]
    if not non_empty_queue_frames:
        review_queue = pd.DataFrame(columns=REVIEW_QUEUE_COLUMNS)
    elif len(non_empty_queue_frames) == 1:
        review_queue = non_empty_queue_frames[0].copy()
    else:
        prepared_frames = [
            _ensure_columns(frame.copy(), REVIEW_QUEUE_COLUMNS)[REVIEW_QUEUE_COLUMNS]
            for frame in non_empty_queue_frames
        ]
        review_queue = pd.DataFrame.from_records(
            [
                record
                for frame in prepared_frames
                for record in frame.to_dict("records")
            ],
            columns=REVIEW_QUEUE_COLUMNS,
        )
    if not review_queue.empty:
        review_queue = review_queue.sort_values(
            by=["transaction_count", "source_system", "source_name_raw"],
            ascending=[False, True, True],
        ).reset_index(drop=True)

    audit_frames = [
        _commercial_audit_summary(source_a_commercial, source_b_commercial_base),
        _residential_audit_summary(source_a_res, source_b_res),
    ]
    non_empty_audit_frames = [frame for frame in audit_frames if not frame.empty]
    if not non_empty_audit_frames:
        audit_summary = pd.DataFrame(columns=AUDIT_SUMMARY_COLUMNS)
    else:
        audit_summary = pd.concat(non_empty_audit_frames, ignore_index=True)

    return review_queue, audit_summary


def _build_candidate_lookup(
    *,
    source_b_commercial_primary: pd.DataFrame,
    source_c_frames: Iterable[pd.DataFrame],
) -> dict[str, tuple[str, str]]:
    lookup: dict[str, tuple[str, str]] = {}

    if "eng_name" in source_b_commercial_primary.columns:
        for name in source_b_commercial_primary["eng_name"].dropna():
            normalized = normalize_building_name(name)
            if normalized and normalized not in lookup:
                lookup[normalized] = (str(name).upper().strip(), "source_b_commercial_primary")

    for frame in source_c_frames:
        name_col = next(
            (
                column
                for column in ("name", "building_name", "title")
                if column in frame.columns
            ),
            None,
        )
        if not name_col:
            continue
        for name in frame[name_col].dropna():
            normalized = normalize_building_name(name)
            if normalized and normalized not in lookup:
                lookup[normalized] = (str(name).upper().strip(), "source_c")

    return lookup


def _build_source_a_res_candidate_rows(source_a_res: pd.DataFrame) -> pd.DataFrame:
    if source_a_res.empty:
        return pd.DataFrame(columns=MEGA_SHEET_COLUMNS["source_a_res"])

    building_code = source_a_res.get("building_code", pd.Series(pd.NA, index=source_a_res.index))
    unmatched_mask = _is_blank_series(building_code)
    unmatched = source_a_res.loc[unmatched_mask].copy()
    if unmatched.empty:
        return pd.DataFrame(columns=MEGA_SHEET_COLUMNS["source_a_res"])

    source_name = unmatched.get("Tower")
    if source_name is None:
        source_name = pd.Series(pd.NA, index=unmatched.index)
    source_name = source_name.where(source_name.astype("string").str.strip().ne(""), unmatched.get("Name"))

    grouped = (
        unmatched.assign(
            source_join_key=building_code.loc[unmatched.index],
            source_name_raw=source_name.astype("string").fillna("").str.strip(),
            normalized_name=source_name.map(normalize_building_name),
            district_name_en=unmatched.get("district", pd.Series(pd.NA, index=unmatched.index)),
            zone_en=unmatched.get("region", pd.Series(pd.NA, index=unmatched.index)),
        )
        .groupby(
            ["source_join_key", "source_name_raw", "normalized_name", "district_name_en", "zone_en"],
            dropna=False,
        )
        .size()
        .reset_index(name="occurrence_count")
    )
    grouped = grouped[
        grouped["source_name_raw"].map(_clean_text).notna()
        & ~grouped["normalized_name"].isin(_BLANK_VALUES)
    ].copy()
    if grouped.empty:
        return pd.DataFrame(columns=MEGA_SHEET_COLUMNS["source_a_res"])
    grouped["row_kind"] = "unmatched_candidate"
    grouped["domain"] = "residential"
    grouped["source_system"] = "source_a_res"
    grouped["district_code"] = pd.NA
    grouped["candidate_building_name"] = pd.NA
    grouped["candidate_source"] = pd.NA
    grouped["native_address"] = pd.NA
    grouped["native_completion_year"] = pd.NA
    grouped["native_management_company"] = pd.NA
    grouped["native_url"] = pd.NA
    grouped["native_detail_source"] = pd.NA
    grouped["manual_canonical_name"] = pd.NA
    grouped["manual_address"] = pd.NA
    grouped["manual_completion_year"] = pd.NA
    grouped["manual_management_company"] = pd.NA
    grouped["manual_url"] = pd.NA
    grouped["manual_notes"] = pd.NA
    grouped["manual_include"] = pd.NA
    grouped["native_developer"] = pd.NA
    grouped["native_blocks"] = pd.NA
    grouped["native_link"] = pd.NA
    return _ensure_columns(grouped, MEGA_SHEET_COLUMNS["source_a_res"])[
        MEGA_SHEET_COLUMNS["source_a_res"]
    ]


def _build_mega_source_a_commercial_candidates(
    source_a_commercial: pd.DataFrame,
    candidate_lookup: dict[str, tuple[str, str]],
) -> pd.DataFrame:
    review_queue = _build_source_a_queue(source_a_commercial, candidate_lookup)
    if review_queue.empty:
        return pd.DataFrame(columns=MEGA_SHEET_COLUMNS["source_a_commercial"])
    frame = pd.DataFrame(
        {
            "row_kind": "unmatched_candidate",
            "domain": review_queue["domain"],
            "source_system": review_queue["source_system"],
            "source_join_key": review_queue["source_join_key"],
            "source_name_raw": review_queue["source_name_raw"],
            "normalized_name": review_queue["normalized_name"],
            "district_code": review_queue["district_code"],
            "district_name_en": review_queue["district_name_en"],
            "zone_en": review_queue["zone_en"],
            "occurrence_count": review_queue["transaction_count"],
            "candidate_building_name": review_queue["candidate_building_name"],
            "candidate_source": review_queue["candidate_source"],
            "native_address": pd.NA,
            "native_completion_year": pd.NA,
            "native_management_company": pd.NA,
            "native_url": pd.NA,
            "native_detail_source": pd.NA,
            "manual_canonical_name": pd.NA,
            "manual_address": pd.NA,
            "manual_completion_year": pd.NA,
            "manual_management_company": pd.NA,
            "manual_url": pd.NA,
            "manual_notes": pd.NA,
            "manual_include": pd.NA,
            "native_grade": pd.NA,
            "native_usage": pd.NA,
            "native_property_type": pd.NA,
            "native_developers": pd.NA,
        }
    )
    return _ensure_columns(frame, MEGA_SHEET_COLUMNS["source_a_commercial"])[
        MEGA_SHEET_COLUMNS["source_a_commercial"]
    ]


def _build_source_b_res_candidate_rows(source_b_res: pd.DataFrame) -> pd.DataFrame:
    if source_b_res.empty:
        return pd.DataFrame(columns=MEGA_SHEET_COLUMNS["source_b_res"])

    building = source_b_res.get("building", pd.Series(pd.NA, index=source_b_res.index))
    unmatched_mask = _is_blank_series(building)
    unmatched = source_b_res.loc[unmatched_mask].copy()
    if unmatched.empty:
        return pd.DataFrame(columns=MEGA_SHEET_COLUMNS["source_b_res"])

    source_name = building.where(building.astype("string").str.strip().ne(""), unmatched.get("estate"))
    grouped = (
        unmatched.assign(
            source_join_key=pd.NA,
            source_name_raw=source_name.astype("string").fillna("").str.strip(),
            normalized_name=source_name.map(normalize_building_name),
            district_name_en=unmatched.get("district", pd.Series(pd.NA, index=unmatched.index)),
            zone_en=unmatched.get("region_name_trans", pd.Series(pd.NA, index=unmatched.index)),
        )
        .groupby(
            ["source_join_key", "source_name_raw", "normalized_name", "district_name_en", "zone_en"],
            dropna=False,
        )
        .size()
        .reset_index(name="occurrence_count")
    )
    grouped = grouped[
        grouped["source_name_raw"].map(_clean_text).notna()
        & ~grouped["normalized_name"].isin(_BLANK_VALUES)
    ].copy()
    if grouped.empty:
        return pd.DataFrame(columns=MEGA_SHEET_COLUMNS["source_b_res"])
    grouped["row_kind"] = "unmatched_candidate"
    grouped["domain"] = "residential"
    grouped["source_system"] = "source_b_res"
    grouped["district_code"] = pd.NA
    grouped["candidate_building_name"] = pd.NA
    grouped["candidate_source"] = pd.NA
    grouped["native_address"] = pd.NA
    grouped["native_completion_year"] = pd.NA
    grouped["native_management_company"] = pd.NA
    grouped["native_url"] = pd.NA
    grouped["native_detail_source"] = pd.NA
    grouped["manual_canonical_name"] = pd.NA
    grouped["manual_address"] = pd.NA
    grouped["manual_completion_year"] = pd.NA
    grouped["manual_management_company"] = pd.NA
    grouped["manual_url"] = pd.NA
    grouped["manual_notes"] = pd.NA
    grouped["manual_include"] = pd.NA
    grouped["native_developer"] = pd.NA
    grouped["native_total_unit_count"] = pd.NA
    grouped["native_total_block_count"] = pd.NA
    return _ensure_columns(grouped, MEGA_SHEET_COLUMNS["source_b_res"])[
        MEGA_SHEET_COLUMNS["source_b_res"]
    ]


def _build_mega_source_b_commercial_candidates(
    source_b_commercial_base: pd.DataFrame,
    candidate_lookup: dict[str, tuple[str, str]],
) -> pd.DataFrame:
    review_queue = _build_source_b_queue(source_b_commercial_base, candidate_lookup)
    if review_queue.empty:
        return pd.DataFrame(columns=MEGA_SHEET_COLUMNS["source_b_commercial"])
    frame = pd.DataFrame(
        {
            "row_kind": "unmatched_candidate",
            "domain": review_queue["domain"],
            "source_system": review_queue["source_system"],
            "source_join_key": review_queue["source_join_key"],
            "source_name_raw": review_queue["source_name_raw"],
            "normalized_name": review_queue["normalized_name"],
            "district_code": review_queue["district_code"],
            "district_name_en": review_queue["district_name_en"],
            "zone_en": review_queue["zone_en"],
            "occurrence_count": review_queue["transaction_count"],
            "candidate_building_name": review_queue["candidate_building_name"],
            "candidate_source": review_queue["candidate_source"],
            "native_address": pd.NA,
            "native_completion_year": pd.NA,
            "native_management_company": pd.NA,
            "native_url": pd.NA,
            "native_detail_source": pd.NA,
            "manual_canonical_name": pd.NA,
            "manual_address": pd.NA,
            "manual_completion_year": pd.NA,
            "manual_management_company": pd.NA,
            "manual_url": pd.NA,
            "manual_notes": pd.NA,
            "manual_include": pd.NA,
            "native_type": pd.NA,
            "native_title": pd.NA,
            "native_total_area": pd.NA,
        }
    )
    return _ensure_columns(frame, MEGA_SHEET_COLUMNS["source_b_commercial"])[
        MEGA_SHEET_COLUMNS["source_b_commercial"]
    ]


def _build_source_a_res_native_rows(frame: pd.DataFrame) -> pd.DataFrame:
    frame = _exclude_manual_writeback_rows(frame)
    if frame.empty:
        return pd.DataFrame(columns=MEGA_SHEET_COLUMNS["source_a_res"])
    native = pd.DataFrame(
        {
            "row_kind": "native",
            "domain": "residential",
            "source_system": "source_a_res",
            "source_join_key": frame.get("estate_code", pd.Series(pd.NA, index=frame.index)),
            "source_name_raw": frame.get("Name", pd.Series(pd.NA, index=frame.index)),
            "normalized_name": frame.get("Name", pd.Series(pd.NA, index=frame.index)).map(normalize_building_name),
            "district_code": pd.NA,
            "district_name_en": frame.get("District", pd.Series(pd.NA, index=frame.index)),
            "zone_en": frame.get("Region", pd.Series(pd.NA, index=frame.index)),
            "occurrence_count": 0,
            "candidate_building_name": pd.NA,
            "candidate_source": pd.NA,
            "native_address": frame.get("estate_detailed_address", pd.Series(pd.NA, index=frame.index)),
            "native_completion_year": frame.get("occupation_permit", pd.Series(pd.NA, index=frame.index)),
            "native_management_company": pd.NA,
            "native_url": frame.get("Link", pd.Series(pd.NA, index=frame.index)),
            "native_detail_source": "source_a_res_buildings",
            "manual_canonical_name": pd.NA,
            "manual_address": pd.NA,
            "manual_completion_year": pd.NA,
            "manual_management_company": pd.NA,
            "manual_url": pd.NA,
            "manual_notes": pd.NA,
            "manual_include": pd.NA,
            "native_developer": frame.get("developer", pd.Series(pd.NA, index=frame.index)),
            "native_blocks": frame.get("scraped_blocks", pd.Series(pd.NA, index=frame.index)),
            "native_link": frame.get("Link", pd.Series(pd.NA, index=frame.index)),
        }
    )
    return _ensure_columns(native, MEGA_SHEET_COLUMNS["source_a_res"])[
        MEGA_SHEET_COLUMNS["source_a_res"]
    ]


def _build_source_a_commercial_native_rows(frame: pd.DataFrame) -> pd.DataFrame:
    frame = _exclude_manual_writeback_rows(frame)
    if frame.empty:
        return pd.DataFrame(columns=MEGA_SHEET_COLUMNS["source_a_commercial"])
    native = pd.DataFrame(
        {
            "row_kind": "native",
            "domain": "commercial",
            "source_system": "source_a_commercial",
            "source_join_key": frame.get("property_id", pd.Series(pd.NA, index=frame.index)),
            "source_name_raw": frame.get("building_name", pd.Series(pd.NA, index=frame.index)),
            "normalized_name": frame.get("building_name", pd.Series(pd.NA, index=frame.index)).map(normalize_building_name),
            "district_code": pd.NA,
            "district_name_en": frame.get("district", pd.Series(pd.NA, index=frame.index)),
            "zone_en": frame.get("zone", pd.Series(pd.NA, index=frame.index)),
            "occurrence_count": 0,
            "candidate_building_name": pd.NA,
            "candidate_source": pd.NA,
            "native_address": frame.get("full_address", pd.Series(pd.NA, index=frame.index)),
            "native_completion_year": frame.get("completion_year", pd.Series(pd.NA, index=frame.index)),
            "native_management_company": frame.get("management_company", pd.Series(pd.NA, index=frame.index)),
            "native_url": frame.get("source_url", pd.Series(pd.NA, index=frame.index)),
            "native_detail_source": "source_a_commercial_buildings",
            "manual_canonical_name": pd.NA,
            "manual_address": pd.NA,
            "manual_completion_year": pd.NA,
            "manual_management_company": pd.NA,
            "manual_url": pd.NA,
            "manual_notes": pd.NA,
            "manual_include": pd.NA,
            "native_grade": frame.get("grade", pd.Series(pd.NA, index=frame.index)),
            "native_usage": frame.get("usage", pd.Series(pd.NA, index=frame.index)),
            "native_property_type": frame.get("property_type", pd.Series(pd.NA, index=frame.index)),
            "native_developers": frame.get("developers", pd.Series(pd.NA, index=frame.index)),
        }
    )
    return _ensure_columns(native, MEGA_SHEET_COLUMNS["source_a_commercial"])[
        MEGA_SHEET_COLUMNS["source_a_commercial"]
    ]


def _build_source_b_res_native_rows(frame: pd.DataFrame) -> pd.DataFrame:
    frame = _exclude_manual_writeback_rows(frame)
    if frame.empty:
        return pd.DataFrame(columns=MEGA_SHEET_COLUMNS["source_b_res"])
    native = pd.DataFrame(
        {
            "row_kind": "native",
            "domain": "residential",
            "source_system": "source_b_res",
            "source_join_key": frame.get("id", pd.Series(pd.NA, index=frame.index)),
            "source_name_raw": frame.get("name", pd.Series(pd.NA, index=frame.index)),
            "normalized_name": frame.get("name", pd.Series(pd.NA, index=frame.index)).map(normalize_building_name),
            "district_code": pd.NA,
            "district_name_en": frame.get("district_name", pd.Series(pd.NA, index=frame.index)),
            "zone_en": frame.get("region_name", pd.Series(pd.NA, index=frame.index)),
            "occurrence_count": 0,
            "candidate_building_name": pd.NA,
            "candidate_source": pd.NA,
            "native_address": frame.get("location", pd.Series(pd.NA, index=frame.index)),
            "native_completion_year": frame.get("first_op_date", pd.Series(pd.NA, index=frame.index)),
            "native_management_company": pd.NA,
            "native_url": frame.get("url_desc", pd.Series(pd.NA, index=frame.index)),
            "native_detail_source": "source_b_res_buildings",
            "manual_canonical_name": pd.NA,
            "manual_address": pd.NA,
            "manual_completion_year": pd.NA,
            "manual_management_company": pd.NA,
            "manual_url": pd.NA,
            "manual_notes": pd.NA,
            "manual_include": pd.NA,
            "native_developer": frame.get("developer_name", pd.Series(pd.NA, index=frame.index)),
            "native_total_unit_count": frame.get("total_unit_count", pd.Series(pd.NA, index=frame.index)),
            "native_total_block_count": frame.get("total_block_count", pd.Series(pd.NA, index=frame.index)),
        }
    )
    return _ensure_columns(native, MEGA_SHEET_COLUMNS["source_b_res"])[
        MEGA_SHEET_COLUMNS["source_b_res"]
    ]


def _build_source_b_commercial_native_rows(frame: pd.DataFrame) -> pd.DataFrame:
    frame = _exclude_manual_writeback_rows(frame)
    if frame.empty:
        return pd.DataFrame(columns=MEGA_SHEET_COLUMNS["source_b_commercial"])
    native = pd.DataFrame(
        {
            "row_kind": "native",
            "domain": "commercial",
            "source_system": "source_b_commercial",
            "source_join_key": frame.get("id", pd.Series(pd.NA, index=frame.index)),
            "source_name_raw": frame.get("Building Name", pd.Series(pd.NA, index=frame.index)),
            "normalized_name": frame.get("Building Name", pd.Series(pd.NA, index=frame.index)).map(normalize_building_name),
            "district_code": pd.NA,
            # district_name_en/zone_en are backfilled by
            # _enrich_source_b_commercial_buildings_with_geo from transaction data,
            # since the native building scrape never captured district/zone directly.
            "district_name_en": frame.get("district_name_en", pd.Series(pd.NA, index=frame.index)),
            "zone_en": frame.get("zone_en", pd.Series(pd.NA, index=frame.index)),
            "occurrence_count": 0,
            "candidate_building_name": pd.NA,
            "candidate_source": pd.NA,
            "native_address": pd.NA,
            "native_completion_year": frame.get("Completion", pd.Series(pd.NA, index=frame.index)),
            "native_management_company": frame.get("Management Company", pd.Series(pd.NA, index=frame.index)),
            "native_url": frame.get("URL", pd.Series(pd.NA, index=frame.index)),
            "native_detail_source": "source_b_commercial_buildings",
            "manual_canonical_name": pd.NA,
            "manual_address": pd.NA,
            "manual_completion_year": pd.NA,
            "manual_management_company": pd.NA,
            "manual_url": pd.NA,
            "manual_notes": pd.NA,
            "manual_include": pd.NA,
            "native_grade": frame.get("Grade", pd.Series(pd.NA, index=frame.index)),
            "native_type": frame.get("Type", pd.Series(pd.NA, index=frame.index)),
            "native_title": frame.get("Title", pd.Series(pd.NA, index=frame.index)),
            "native_total_area": frame.get("Total Area", pd.Series(pd.NA, index=frame.index)),
        }
    )
    return _ensure_columns(native, MEGA_SHEET_COLUMNS["source_b_commercial"])[
        MEGA_SHEET_COLUMNS["source_b_commercial"]
    ]


def _build_source_a_queue(
    source_a_commercial: pd.DataFrame,
    candidate_lookup: dict[str, tuple[str, str]],
) -> pd.DataFrame:
    if source_a_commercial.empty or "_match_method" not in source_a_commercial.columns:
        return pd.DataFrame(columns=REVIEW_QUEUE_COLUMNS)

    unmatched = source_a_commercial[source_a_commercial["_match_method"] == "unmatched"].copy()
    if unmatched.empty or "propertyNameEn" not in unmatched.columns:
        return pd.DataFrame(columns=REVIEW_QUEUE_COLUMNS)

    grouped = (
        unmatched.assign(
            source_name_raw=unmatched["propertyNameEn"].astype("string").str.strip(),
            normalized_name=unmatched["propertyNameEn"].map(normalize_building_name),
        )
        .groupby(
            ["source_name_raw", "normalized_name", "districtNameEn", "zoneEn"],
            dropna=False,
        )
        .size()
        .reset_index(name="transaction_count")
    )
    grouped = grouped[
        grouped["source_name_raw"].map(_clean_text).notna()
        & ~grouped["normalized_name"].isin(_BLANK_VALUES)
    ].copy()

    grouped["domain"] = "commercial"
    grouped["source_system"] = "source_a_commercial"
    grouped["source_join_key"] = pd.NA
    grouped["district_code"] = pd.NA
    grouped["district_name_en"] = grouped["districtNameEn"].astype("string")
    grouped["zone_en"] = grouped["zoneEn"].astype("string")
    grouped["review_status"] = "pending"
    grouped["review_notes"] = pd.NA
    grouped["canonical_building_name"] = pd.NA
    grouped["address"] = pd.NA
    grouped["completion_year"] = pd.NA
    grouped["management_company"] = pd.NA
    grouped["url"] = pd.NA
    grouped["match_origin"] = pd.NA
    grouped["record_source"] = pd.NA
    grouped["candidate_building_name"] = grouped["normalized_name"].map(
        lambda value: candidate_lookup.get(value, (pd.NA, pd.NA))[0]
    )
    grouped["candidate_source"] = grouped["normalized_name"].map(
        lambda value: candidate_lookup.get(value, (pd.NA, pd.NA))[1]
    )
    return grouped[REVIEW_QUEUE_COLUMNS]


def _build_source_b_queue(
    source_b_commercial_base: pd.DataFrame,
    candidate_lookup: dict[str, tuple[str, str]],
) -> pd.DataFrame:
    if source_b_commercial_base.empty:
        return pd.DataFrame(columns=REVIEW_QUEUE_COLUMNS)

    if "has_building_match" in source_b_commercial_base.columns:
        unmatched_mask = source_b_commercial_base["has_building_match"].fillna(False).ne(True)
    else:
        unmatched_mask = pd.Series(True, index=source_b_commercial_base.index)
    if "Building Name" in source_b_commercial_base.columns:
        unmatched_mask |= _is_blank_series(source_b_commercial_base["Building Name"])
    unmatched = source_b_commercial_base[unmatched_mask].copy()
    if unmatched.empty:
        return pd.DataFrame(columns=REVIEW_QUEUE_COLUMNS)

    source_name = unmatched.get("eng_name")
    if source_name is None:
        source_name = unmatched.get("chi_name", pd.Series(pd.NA, index=unmatched.index))

    grouped = (
        unmatched.assign(
            source_name_raw=source_name.astype("string").fillna("").str.strip(),
            normalized_name=source_name.map(normalize_building_name),
            source_join_key=unmatched.get("building_id", pd.Series(pd.NA, index=unmatched.index)),
        )
        .groupby(
            ["source_join_key", "source_name_raw", "normalized_name", "dist_code", "dist_name_en"],
            dropna=False,
        )
        .size()
        .reset_index(name="transaction_count")
    )
    grouped = grouped[
        grouped["source_name_raw"].map(_clean_text).notna()
        & ~grouped["normalized_name"].isin(_BLANK_VALUES)
    ].copy()

    # Reuses the authoritative Source B district->zone mapping already maintained
    # for the main data_process pipeline, so the two stay in sync.
    from ..pipelines.data_process.nodes import _source_b_zone_from_dist_code

    grouped["domain"] = "commercial"
    grouped["source_system"] = "source_b_commercial"
    grouped["district_code"] = grouped["dist_code"].astype("string")
    grouped["district_name_en"] = grouped["dist_name_en"].astype("string")
    grouped["zone_en"] = grouped.apply(
        lambda row: _source_b_zone_from_dist_code(row["dist_code"], row["dist_name_en"]),
        axis=1,
    )
    grouped["review_status"] = "pending"
    grouped["review_notes"] = pd.NA
    grouped["canonical_building_name"] = pd.NA
    grouped["address"] = pd.NA
    grouped["completion_year"] = pd.NA
    grouped["management_company"] = pd.NA
    grouped["url"] = pd.NA
    grouped["match_origin"] = pd.NA
    grouped["record_source"] = pd.NA
    grouped["candidate_building_name"] = grouped["normalized_name"].map(
        lambda value: candidate_lookup.get(value, (pd.NA, pd.NA))[0]
    )
    grouped["candidate_source"] = grouped["normalized_name"].map(
        lambda value: candidate_lookup.get(value, (pd.NA, pd.NA))[1]
    )
    return grouped[REVIEW_QUEUE_COLUMNS]


def _commercial_audit_summary(
    source_a_commercial: pd.DataFrame,
    source_b_commercial_base: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    if not source_a_commercial.empty and "_match_method" in source_a_commercial.columns:
        counts = source_a_commercial["_match_method"].fillna("missing").value_counts()
        for status, row_count in counts.items():
            rows.append(
                {
                    "domain": "commercial",
                    "source_system": "source_a_commercial",
                    "metric": "match_method",
                    "status": status,
                    "row_count": int(row_count),
                }
            )

    if not source_b_commercial_base.empty and "has_building_match" in source_b_commercial_base.columns:
        counts = (
            source_b_commercial_base["has_building_match"]
            .map({True: "matched", False: "unmatched"})
            .fillna("missing")
            .value_counts()
        )
        for status, row_count in counts.items():
            rows.append(
                {
                    "domain": "commercial",
                    "source_system": "source_b_commercial",
                    "metric": "building_match",
                    "status": status,
                    "row_count": int(row_count),
                }
            )

    return pd.DataFrame(rows, columns=AUDIT_SUMMARY_COLUMNS)


def _residential_audit_summary(
    source_a_res: Optional[pd.DataFrame],
    source_b_res: Optional[pd.DataFrame],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    if source_a_res is not None and not source_a_res.empty:
        rows.extend(
            [
                {
                    "domain": "residential",
                    "source_system": "source_a_res",
                    "metric": "missing_tower_rows",
                    "status": "missing",
                    "row_count": int(_nullish_count(source_a_res, "Tower")),
                },
                {
                    "domain": "residential",
                    "source_system": "source_a_res",
                    "metric": "missing_building_code_rows",
                    "status": "missing",
                    "row_count": int(_nullish_count(source_a_res, "building_code")),
                },
            ]
        )

    if source_b_res is not None and not source_b_res.empty:
        rows.append(
            {
                "domain": "residential",
                "source_system": "source_b_res",
                "metric": "missing_building_rows",
                "status": "missing",
                "row_count": int(_nullish_count(source_b_res, "building")),
            }
        )

    return pd.DataFrame(rows, columns=AUDIT_SUMMARY_COLUMNS)


def _read_excel_sheet(
    workbook_path: Path,
    sheet_name: str,
    columns: list[str],
) -> pd.DataFrame:
    try:
        frame = pd.read_excel(workbook_path, sheet_name=sheet_name)
    except ValueError:
        return pd.DataFrame(columns=columns)
    except FileNotFoundError:
        return pd.DataFrame(columns=columns)
    return _ensure_columns(frame, columns)


def _ensure_columns(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame[columns + [col for col in frame.columns if col not in columns]]


def _dedupe_master_rows(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame

    keyed = frame.copy()
    keyed["_dedupe_key"] = keyed.apply(
        lambda row: (
            f"key::{row.get('source_system')}::{_clean_key(row.get('source_join_key'))}"
            if _clean_key(row.get("source_join_key"))
            else "name::"
            f"{row.get('source_system')}::{normalize_building_name(row.get('normalized_name') or row.get('canonical_building_name'))}"
            f"::{_clean_text(row.get('district_name_en'))}::{_clean_text(row.get('zone_en'))}"
        ),
        axis=1,
    )
    return keyed.drop_duplicates(subset=["_dedupe_key"], keep="first").drop(
        columns=["_dedupe_key"]
    )


def _merge_mega_candidate_rows(
    *,
    fresh_candidates: pd.DataFrame,
    existing_sheet: pd.DataFrame,
    source_system: str,
    include_limit: int,
) -> pd.DataFrame:
    columns = MEGA_SHEET_COLUMNS[source_system]
    fresh = _ensure_columns(fresh_candidates.copy(), columns) if not fresh_candidates.empty else pd.DataFrame(columns=columns)
    existing = _ensure_columns(existing_sheet.copy(), columns) if not existing_sheet.empty else pd.DataFrame(columns=columns)
    existing_candidates = existing[existing["row_kind"] != "native"].copy() if not existing.empty else pd.DataFrame(columns=columns)

    manual_cols = [
        "manual_canonical_name",
        "manual_address",
        "manual_completion_year",
        "manual_management_company",
        "manual_url",
        "manual_notes",
        "manual_include",
    ]

    if not existing_candidates.empty:
        existing_candidates["_row_key"] = existing_candidates.apply(_mega_row_key, axis=1)
    if not fresh.empty:
        fresh["_row_key"] = fresh.apply(_mega_row_key, axis=1)

    carried = []
    if not existing_candidates.empty:
        existing_map = (
            existing_candidates.drop_duplicates("_row_key", keep="last")
            .set_index("_row_key")
        )
        fresh_keys = set(fresh["_row_key"]) if not fresh.empty else set()
        for key, existing_row in existing_map.iterrows():
            if key not in fresh_keys:
                carried.append(existing_row.to_dict())

        if not fresh.empty:
            existing_manual = existing_candidates[
                ["_row_key", *manual_cols]
            ].drop_duplicates("_row_key", keep="last")
            fresh = fresh.merge(
                existing_manual,
                on="_row_key",
                how="left",
                suffixes=("", "_old"),
            )
            for column in manual_cols:
                old_col = f"{column}_old"
                if old_col in fresh.columns:
                    fresh[column] = fresh[old_col].where(
                        fresh[old_col].map(_clean_text).notna()
                        if column != "manual_include"
                        else fresh[old_col].map(_is_truthy_value),
                        fresh[column],
                    )
            fresh = fresh.drop(
                columns=[col for col in fresh.columns if col.endswith("_old")],
                errors="ignore",
            )

    combined_records = []
    if not fresh.empty:
        combined_records.extend(_ensure_columns(fresh.copy(), columns).to_dict("records"))
    if carried:
        combined_records.extend(
            _ensure_columns(pd.DataFrame(carried), columns).to_dict("records")
        )
    combined = (
        pd.DataFrame.from_records(combined_records, columns=columns)
        if combined_records
        else pd.DataFrame(columns=columns)
    )
    if combined.empty:
        return combined

    combined["row_kind"] = combined.apply(
        lambda row: "manual_approved"
        if _is_truthy_value(row.get("manual_include"))
        and _clean_text(row.get("manual_canonical_name")) is not None
        else "unmatched_candidate",
        axis=1,
    )
    combined = _ensure_columns(combined, columns)
    if include_limit > 0:
        combined = _apply_mega_tab_limit(combined, include_limit, source_system)
    return combined[columns]


def _apply_mega_tab_limit(
    candidate_rows: pd.DataFrame,
    include_limit: int,
    source_system: str,
) -> pd.DataFrame:
    if include_limit <= 0 or candidate_rows.empty:
        return candidate_rows
    touched_mask = candidate_rows["manual_include"].map(_is_truthy_value)
    touched_mask |= candidate_rows["manual_canonical_name"].map(_clean_text).notna()
    touched_mask |= candidate_rows["manual_notes"].map(_clean_text).notna()
    touched_rows = candidate_rows[touched_mask].copy()
    untouched_rows = candidate_rows[~touched_mask].copy()
    untouched_rows = untouched_rows.sort_values(
        by=["occurrence_count", "source_name_raw"],
        ascending=[False, True],
    ).head(max(include_limit - len(touched_rows), 0))
    combined = pd.concat([touched_rows, untouched_rows], ignore_index=True)
    combined["_row_key"] = combined.apply(_mega_row_key, axis=1)
    return (
        combined.drop_duplicates("_row_key", keep="first")
        .drop(columns=["_row_key"], errors="ignore")
        .reset_index(drop=True)
    )


def _sort_mega_sheet_rows(frame: pd.DataFrame, *, source_system: str) -> pd.DataFrame:
    if frame.empty:
        return _ensure_columns(frame, MEGA_SHEET_COLUMNS[source_system])[MEGA_SHEET_COLUMNS[source_system]]
    prepared = _ensure_columns(frame.copy(), MEGA_SHEET_COLUMNS[source_system])
    prepared["_row_order"] = prepared["row_kind"].map(
        {"manual_approved": 0, "unmatched_candidate": 1, "native": 2}
    ).fillna(9)
    prepared["_count_sort"] = pd.to_numeric(
        prepared["occurrence_count"],
        errors="coerce",
    ).fillna(0)
    return (
        prepared.sort_values(
            by=["_row_order", "_count_sort", "source_name_raw"],
            ascending=[True, False, True],
        )
        .drop(columns=["_row_order", "_count_sort"], errors="ignore")
        .reset_index(drop=True)
    )


def _mega_row_key(row: pd.Series) -> str:
    join_key = _clean_key(row.get("source_join_key"))
    if join_key:
        return f"{row.get('source_system')}::key::{join_key}"
    return "::".join(
        [
            str(row.get("source_system")),
            normalize_building_name(row.get("normalized_name") or row.get("source_name_raw")),
            str(_clean_text(row.get("district_name_en")) or ""),
            str(_clean_text(row.get("zone_en")) or ""),
        ]
    )


def _is_truthy_value(value: Any) -> bool:
    if value is None or pd.isna(value):
        return False
    text = str(value).strip().lower()
    return text in {"y", "yes", "true", "1", "manual", "approved"}


def _exclude_manual_writeback_rows(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    filtered = frame.copy()
    if "record_source" in filtered.columns:
        filtered = filtered[
            filtered["record_source"].fillna("").astype("string").str.lower().ne(MANUAL_RECORD_SOURCE)
        ].copy()
    if "is_manual_record" in filtered.columns:
        filtered = filtered[~filtered["is_manual_record"].fillna(False)].copy()
    return filtered.reset_index(drop=True)


def _merge_manual_rows_for_source(
    *,
    base_frame: pd.DataFrame,
    manual_rows: pd.DataFrame,
    row_key_builder: Any,
) -> pd.DataFrame:
    base_without_manual = _exclude_manual_writeback_rows(base_frame)
    if base_without_manual.empty and manual_rows.empty:
        return pd.DataFrame()

    combined = pd.concat(
        [frame for frame in (base_without_manual, manual_rows) if not frame.empty],
        ignore_index=True,
        sort=False,
    )
    if combined.empty:
        return combined

    combined["_manual_row_key"] = combined.apply(row_key_builder, axis=1)
    return (
        combined.drop_duplicates(subset=["_manual_row_key"], keep="last")
        .drop(columns=["_manual_row_key"], errors="ignore")
        .reset_index(drop=True)
    )


def _build_source_a_commercial_manual_rows(master: pd.DataFrame) -> pd.DataFrame:
    source_rows = master[master["source_system"] == "source_a_commercial"].copy()
    if source_rows.empty:
        return pd.DataFrame()
    building_names = source_rows["canonical_building_name"].where(
        source_rows["canonical_building_name"].map(_clean_text).notna(),
        source_rows["source_name_raw"],
    )
    return pd.DataFrame(
        {
            "property_id": source_rows["source_join_key"],
            "building_name": building_names,
            "district": source_rows["district_name_en"],
            "zone": source_rows["zone_en"],
            "full_address": source_rows["address"],
            "completion_year": source_rows["completion_year"],
            "management_company": source_rows["management_company"],
            "source_url": source_rows["url"],
            "grade": pd.NA,
            "usage": pd.NA,
            "property_type": pd.NA,
            "developers": pd.NA,
            "match_origin": "manual",
            "record_source": MANUAL_RECORD_SOURCE,
            "is_manual_record": True,
            "manual_source_name_raw": source_rows["source_name_raw"],
            "manual_normalized_name": source_rows["normalized_name"].where(
                source_rows["normalized_name"].map(_clean_text).notna(),
                building_names.map(normalize_building_name),
            ),
        }
    )


def _build_source_b_commercial_manual_rows(master: pd.DataFrame) -> pd.DataFrame:
    source_rows = master[master["source_system"] == "source_b_commercial"].copy()
    if source_rows.empty:
        return pd.DataFrame()
    building_names = source_rows["canonical_building_name"].where(
        source_rows["canonical_building_name"].map(_clean_text).notna(),
        source_rows["source_name_raw"],
    )
    return pd.DataFrame(
        {
            "id": source_rows["source_join_key"],
            "Building Name": building_names,
            "Completion": source_rows["completion_year"],
            "Management Company": source_rows["management_company"],
            "URL": source_rows["url"],
            "Type": pd.NA,
            "Title": pd.NA,
            "Total Area": pd.NA,
            "dist_name_en": source_rows["district_name_en"],
            "match_origin": "manual",
            "record_source": MANUAL_RECORD_SOURCE,
            "is_manual_record": True,
            "manual_source_name_raw": source_rows["source_name_raw"],
            "manual_normalized_name": source_rows["normalized_name"].where(
                source_rows["normalized_name"].map(_clean_text).notna(),
                building_names.map(normalize_building_name),
            ),
        }
    )


def _build_source_a_res_manual_rows(master: pd.DataFrame) -> pd.DataFrame:
    source_rows = master[master["source_system"] == "source_a_res"].copy()
    if source_rows.empty:
        return pd.DataFrame()
    building_names = source_rows["canonical_building_name"].where(
        source_rows["canonical_building_name"].map(_clean_text).notna(),
        source_rows["source_name_raw"],
    )
    return pd.DataFrame(
        {
            "estate_code": source_rows["source_join_key"],
            "Name": building_names,
            "District": source_rows["district_name_en"],
            "Region": source_rows["zone_en"],
            "estate_detailed_address": source_rows["address"],
            "occupation_permit": source_rows["completion_year"],
            "developer": pd.NA,
            "scraped_blocks": pd.NA,
            "Link": source_rows["url"],
            "match_origin": "manual",
            "record_source": MANUAL_RECORD_SOURCE,
            "is_manual_record": True,
            "manual_source_name_raw": source_rows["source_name_raw"],
            "manual_normalized_name": source_rows["normalized_name"].where(
                source_rows["normalized_name"].map(_clean_text).notna(),
                building_names.map(normalize_building_name),
            ),
        }
    )


def _build_source_b_res_manual_rows(master: pd.DataFrame) -> pd.DataFrame:
    source_rows = master[master["source_system"] == "source_b_res"].copy()
    if source_rows.empty:
        return pd.DataFrame()
    building_names = source_rows["canonical_building_name"].where(
        source_rows["canonical_building_name"].map(_clean_text).notna(),
        source_rows["source_name_raw"],
    )
    return pd.DataFrame(
        {
            "id": source_rows["source_join_key"],
            "name": building_names,
            "district_name": source_rows["district_name_en"],
            "region_name": source_rows["zone_en"],
            "location": source_rows["address"],
            "first_op_date": source_rows["completion_year"],
            "developer_name": pd.NA,
            "total_unit_count": pd.NA,
            "total_block_count": pd.NA,
            "url_desc": source_rows["url"],
            "match_origin": "manual",
            "record_source": MANUAL_RECORD_SOURCE,
            "is_manual_record": True,
            "manual_source_name_raw": source_rows["source_name_raw"],
            "manual_normalized_name": source_rows["normalized_name"].where(
                source_rows["normalized_name"].map(_clean_text).notna(),
                building_names.map(normalize_building_name),
            ),
        }
    )


def _source_a_commercial_building_key(row: pd.Series) -> str:
    join_key = _clean_key(row.get("property_id"))
    if join_key:
        return f"source_a_commercial::key::{join_key}"
    return "::".join(
        [
            "source_a_commercial",
            normalize_building_name(row.get("manual_normalized_name") or row.get("building_name")),
            str(_clean_text(row.get("district")) or ""),
            str(_clean_text(row.get("zone")) or ""),
        ]
    )


def _source_b_commercial_building_key(row: pd.Series) -> str:
    join_key = _clean_key(row.get("id"))
    if join_key:
        return f"source_b_commercial::key::{join_key}"
    return "::".join(
        [
            "source_b_commercial",
            normalize_building_name(row.get("manual_normalized_name") or row.get("Building Name")),
            str(_clean_text(row.get("dist_name_en")) or ""),
            "",
        ]
    )


def _source_a_res_building_key(row: pd.Series) -> str:
    join_key = _clean_key(row.get("estate_code"))
    if join_key:
        return f"source_a_res::key::{join_key}"
    return "::".join(
        [
            "source_a_res",
            normalize_building_name(row.get("manual_normalized_name") or row.get("Name")),
            str(_clean_text(row.get("District")) or ""),
            str(_clean_text(row.get("Region")) or ""),
        ]
    )


def _source_b_res_building_key(row: pd.Series) -> str:
    join_key = _clean_key(row.get("id"))
    if join_key:
        return f"source_b_res::key::{join_key}"
    return "::".join(
        [
            "source_b_res",
            normalize_building_name(row.get("manual_normalized_name") or row.get("name")),
            str(_clean_text(row.get("district_name")) or ""),
            str(_clean_text(row.get("region_name")) or ""),
        ]
    )


def _sanitize_for_excel(frame: pd.DataFrame) -> pd.DataFrame:
    sanitized = frame.copy()
    for column in sanitized.columns:
        sanitized[column] = sanitized[column].map(_sanitize_excel_value)
    return sanitized


def _sanitize_excel_value(value: Any) -> Any:
    if isinstance(value, pd.Timestamp) and value.tzinfo is not None:
        return value.tz_localize(None)
    if hasattr(value, "tzinfo") and getattr(value, "tzinfo", None) is not None:
        try:
            return value.replace(tzinfo=None)
        except Exception:
            return value
    if isinstance(value, str):
        return _EXCEL_ILLEGAL_CHAR_RE.sub("", value)
    return value


def _first_non_blank_value(series: pd.Series) -> Optional[str]:
    cleaned = series.dropna().astype("string").str.strip()
    cleaned = cleaned[cleaned.ne("")]
    if cleaned.empty:
        return None
    return str(cleaned.iloc[0])


def _merge_first_match(
    working: pd.DataFrame,
    master: pd.DataFrame,
    *,
    left_on: list[str],
    right_on: list[str],
    method: str,
    output_match_method_col: str,
) -> pd.DataFrame:
    prefixed_master = master.rename(
        columns={
            column: f"_supp_{column}"
            for column in master.columns
            if column not in right_on and not column.startswith("_supp_")
        }
    )
    joined = working.merge(
        prefixed_master,
        left_on=left_on,
        right_on=right_on,
        how="inner",
    )
    if joined.empty:
        return pd.DataFrame()

    joined = joined.drop_duplicates(subset=["_supp_original_index"], keep="first")
    joined[output_match_method_col] = method
    joined["matched_building_name"] = joined["_supp_canonical_building_name"].where(
        joined["_supp_canonical_building_name"].map(bool),
        joined["_supp_candidate_building_name"],
    )
    joined["supplement_candidate_source"] = joined.get(
        "_supp_candidate_source",
        pd.Series(pd.NA, index=joined.index),
    )
    joined["supplement_review_status"] = joined.get(
        "_supp_review_status",
        pd.Series(pd.NA, index=joined.index),
    )

    metadata_columns = {
        "_supp_domain",
        "_supp_source_system",
        "_supp_source_join_key",
        "_supp_source_name_raw",
        "_supp_normalized_name",
        "_supp_district_code",
        "_supp_district_name_en",
        "_supp_zone_en",
        "_supp_candidate_building_name",
        "_supp_candidate_source",
        "_supp_review_status",
        "_supp_review_notes",
        "_supp_canonical_building_name",
    }
    for column in joined.columns:
        if not column.startswith("_supp_") or column in metadata_columns:
            continue
        target = column.replace("_supp_", "", 1)
        if target not in joined.columns:
            joined[target] = joined[column]
        else:
            joined[target] = _fill_from_series(joined[target], joined[column])

    return (
        joined.drop(
            columns=[
                col
                for col in joined.columns
                if col.startswith("_supp_") and col != "_supp_original_index"
            ],
            errors="ignore",
        )
        .reset_index(drop=True)
    )


def _fill_from_series(base: pd.Series, fallback: pd.Series) -> pd.Series:
    return base.where(~_is_blank_series(base), fallback)


def _is_blank_series(series: pd.Series) -> pd.Series:
    return series.isna() | (
        series.astype("string").str.strip().str.upper().isin(_BLANK_VALUES)
    )


def _nullish_count(frame: pd.DataFrame, column: str) -> int:
    if column not in frame.columns:
        return 0
    return int(_is_blank_series(frame[column]).sum())


def _clean_text(value: Any) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _clean_key(value: Any) -> Optional[str]:
    text = _clean_text(value)
    return text if text and text.upper() not in _BLANK_VALUES else None


def _empty_supplemental_master() -> pd.DataFrame:
    return pd.DataFrame(columns=SUPPLEMENTAL_MASTER_COLUMNS)
