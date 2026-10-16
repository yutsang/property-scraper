"""Pure Phase 2 triage for the building-master human review queue.

This module deliberately prepares review work only.  It neither approves
matches nor writes reviewed overrides or source records.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from typing import Any

import pandas as pd

from .contracts import OVERRIDE_COLUMNS

_MISSING_TEXT = {"", "N/A", "NA", "NAN", "NONE", "NULL", "<NA>"}
_DEFAULTS: dict[str, Any] = {
    "high_priority_cutoff": 30.0,
    "medium_priority_cutoff": 10.0,
    "address_evidence_weight": 10.0,
    "completion_evidence_weight": 10.0,
    "fuzzy_runner_up_margin": 5.0,
}
_REQUIRED_QUEUE_COLUMNS = {
    "review_id",
    "source_record_id",
    "candidate_source_record_id",
}
_REQUIRED_SOURCE_COLUMNS = {"source_record_id"}
_EVIDENCE_COLUMNS = [
    "source_observed_count",
    "candidate_observed_count",
    "transaction_evidence",
    "dual_address_evidence",
    "dual_completion_evidence",
    "source_address",
    "candidate_address",
    "source_completion_year",
    "candidate_completion_year",
    "source_property_type",
    "candidate_property_type",
    "source_native_parent_id",
    "candidate_native_parent_id",
]
# The first six columns are exactly OVERRIDE_COLUMNS, in order, so a filled-in
# template can be saved straight to conf/base/curated as the overrides file. The
# rest is evidence for whoever (or whatever) is deciding.
_TEMPLATE_COLUMNS = [
    *OVERRIDE_COLUMNS,
    "review_id",
    "candidate_source_record_id",
    "review_category",
    "priority_score",
    "priority_tier",
    *_EVIDENCE_COLUMNS,
]
_SUMMARY_COLUMNS = [
    "review_category",
    "priority_tier",
    "review_count",
    "transaction_evidence",
]


def _params(params: Mapping[str, Any] | None) -> dict[str, Any]:
    """Merge flat or ``building_master_review`` parameters with defaults."""
    config = dict(_DEFAULTS)
    supplied = dict(params or {})
    nested = supplied.get("building_master_review")
    if isinstance(nested, Mapping):
        supplied = {**supplied, **nested}
    aliases = {
        "high_priority_cutoff": ("high_priority_cutoff", "priority_high_cutoff"),
        "medium_priority_cutoff": ("medium_priority_cutoff", "priority_medium_cutoff"),
    }
    for key, names in aliases.items():
        for name in names:
            if name in supplied and supplied[name] is not None:
                config[key] = supplied[name]
                break
    for key in (
        "address_evidence_weight",
        "completion_evidence_weight",
        "fuzzy_runner_up_margin",
    ):
        if key in supplied and supplied[key] is not None:
            config[key] = supplied[key]
    if float(config["medium_priority_cutoff"]) > float(config["high_priority_cutoff"]):
        raise ValueError("medium_priority_cutoff cannot exceed high_priority_cutoff")
    return config


def _text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    result = str(value).strip()
    return "" if result.upper() in _MISSING_TEXT else result


def _key(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]+", "", _text(value).upper())


def _as_number(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if pd.isna(result) else max(result, 0.0)


def _year_conflicts(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    left_year, right_year = (
        _text(left.get("completion_year")),
        _text(right.get("completion_year")),
    )
    return bool(left_year and right_year and left_year != right_year)


def _field_conflicts(
    left: Mapping[str, Any], right: Mapping[str, Any], field: str
) -> bool:
    left_value, right_value = _key(left.get(field)), _key(right.get(field))
    return bool(left_value and right_value and left_value != right_value)


def _reason(row: Mapping[str, Any]) -> str:
    return _text(row.get("reason")).lower()


def _is_fuzzy_margin(row: Mapping[str, Any], config: Mapping[str, Any]) -> bool:
    reason = _reason(row)
    score, runner_up = _as_number(row.get("match_score")), row.get("runner_up_score")
    return bool(
        "margin" in reason
        or "runner-up" in reason
        or (
            pd.notna(runner_up)
            and score - _as_number(runner_up) < float(config["fuzzy_runner_up_margin"])
        )
    )


def _category(
    row: Mapping[str, Any],
    source: Mapping[str, Any],
    candidate: Mapping[str, Any],
    config: Mapping[str, Any],
) -> str:
    """Classify with a fixed precedence so each review item has one category."""
    reason = _reason(row)
    category = "other"
    if _year_conflicts(source, candidate):
        category = "completion_year_conflict"
    elif _field_conflicts(source, candidate, "property_type"):
        category = "property_type_conflict"
    elif _field_conflicts(source, candidate, "normalized_address") or _field_conflicts(
        source, candidate, "address_en"
    ):
        category = "address_conflict"
    elif _field_conflicts(source, candidate, "native_parent_id"):
        category = "parent_conflict"
    if _is_fuzzy_margin(row, config):
        category = "fuzzy_margin"
    if (
        "ambiguous" in reason
        or "collision" in reason
        or bool(row.get("is_ambiguous", False))
    ):
        category = "ambiguous_name_collision"
    if "override" in reason and ("conflict" in reason or "conflicting" in reason):
        category = "override_conflict"
    return category


def _priority_tier(score: float, config: Mapping[str, Any]) -> str:
    if score >= float(config["high_priority_cutoff"]):
        return "high"
    if score >= float(config["medium_priority_cutoff"]):
        return "medium"
    return "low"


def _evidence(
    source: Mapping[str, Any], candidate: Mapping[str, Any], config: Mapping[str, Any]
) -> dict[str, Any]:
    source_count = _as_number(source.get("observed_count"))
    candidate_count = _as_number(candidate.get("observed_count"))
    dual_address = bool(
        _text(source.get("address_en")) and _text(candidate.get("address_en"))
    )
    dual_completion = bool(
        _text(source.get("completion_year")) and _text(candidate.get("completion_year"))
    )
    transaction_evidence = source_count + candidate_count
    score = transaction_evidence
    if dual_address:
        score += float(config["address_evidence_weight"])
    if dual_completion:
        score += float(config["completion_evidence_weight"])
    return {
        "source_observed_count": source_count,
        "candidate_observed_count": candidate_count,
        "transaction_evidence": transaction_evidence,
        "dual_address_evidence": dual_address,
        "dual_completion_evidence": dual_completion,
        "source_address": source.get("address_en", pd.NA),
        "candidate_address": candidate.get("address_en", pd.NA),
        "source_completion_year": source.get("completion_year", pd.NA),
        "candidate_completion_year": candidate.get("completion_year", pd.NA),
        "source_property_type": source.get("property_type", pd.NA),
        "candidate_property_type": candidate.get("property_type", pd.NA),
        "source_native_parent_id": source.get("native_parent_id", pd.NA),
        "candidate_native_parent_id": candidate.get("native_parent_id", pd.NA),
        "priority_score": score,
    }


def _empty_outputs(
    review_queue: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    return (
        review_queue.assign(
            review_category=pd.Series(dtype="object"),
            priority_score=pd.Series(dtype="float64"),
            priority_tier=pd.Series(dtype="object"),
            **{column: pd.Series(dtype="object") for column in _EVIDENCE_COLUMNS},
        ),
        pd.DataFrame(columns=_TEMPLATE_COLUMNS),
        pd.DataFrame(columns=_SUMMARY_COLUMNS),
    )



def _proposed_canonical_id(queue_row: Mapping[str, Any]) -> str:
    """Suggest one canonical id for a pair, derived from the two record ids.

    Deterministic, so re-running the pipeline proposes the same id and a decision
    recorded against it stays valid.
    """
    left = str(queue_row["source_record_id"])
    right = str(queue_row["candidate_source_record_id"])
    digest = hashlib.sha256("|".join(sorted((left, right))).encode("utf-8"))
    return f"building-review-{digest.hexdigest()[:20]}"

def triage_building_review_queue(
    review_queue: pd.DataFrame,
    source_records: pd.DataFrame,
    params: Mapping[str, Any] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Triage a review queue without changing matching decisions or source data."""
    missing_queue = _REQUIRED_QUEUE_COLUMNS.difference(review_queue.columns)
    if missing_queue:
        raise ValueError(
            f"review_queue missing required columns: {sorted(missing_queue)}"
        )
    missing_source = _REQUIRED_SOURCE_COLUMNS.difference(source_records.columns)
    if missing_source:
        raise ValueError(
            f"source_records missing required columns: {sorted(missing_source)}"
        )
    if (
        review_queue["review_id"].isna().any()
        or review_queue["review_id"].duplicated().any()
    ):
        raise ValueError("review_id must be present and unique")
    if (
        source_records["source_record_id"].isna().any()
        or source_records["source_record_id"].duplicated().any()
    ):
        raise ValueError("source_record_id must be present and unique")
    if review_queue.empty:
        return _empty_outputs(review_queue)

    config = _params(params)
    source_by_id = {
        _text(record["source_record_id"]): record
        for record in source_records.to_dict(orient="records")
    }
    triaged_rows: list[dict[str, Any]] = []
    template_rows: list[dict[str, Any]] = []
    for queue_row in review_queue.to_dict(orient="records"):
        source = source_by_id.get(_text(queue_row["source_record_id"]), {})
        candidate = source_by_id.get(_text(queue_row["candidate_source_record_id"]), {})
        evidence = _evidence(source, candidate, config)
        category = _category(queue_row, source, candidate, config)
        tier = _priority_tier(float(evidence["priority_score"]), config)
        triaged_rows.append(
            {
                **queue_row,
                **evidence,
                "review_category": category,
                "priority_tier": tier,
            }
        )
        # A merge needs both sides to name the same canonical id, so the pair
        # gets two rows carrying one proposed id. Only review_status is left to
        # fill in: "approved" merges them, "rejected" records that they differ.
        proposed = _proposed_canonical_id(queue_row)
        for record_id in (
            queue_row["source_record_id"],
            queue_row["candidate_source_record_id"],
        ):
            template_rows.append(
                {
                    "source_record_id": record_id,
                    "canonical_building_id": proposed,
                    "review_status": pd.NA,
                    "reviewed_by": pd.NA,
                    "reviewed_at": pd.NA,
                    "notes": pd.NA,
                    "review_id": queue_row["review_id"],
                    "candidate_source_record_id": queue_row["candidate_source_record_id"],
                    "review_category": category,
                    "priority_score": evidence["priority_score"],
                    "priority_tier": tier,
                    **{column: evidence[column] for column in _EVIDENCE_COLUMNS},
                }
            )

    triaged = pd.DataFrame(triaged_rows).reset_index(drop=True)
    template = pd.DataFrame(template_rows, columns=_TEMPLATE_COLUMNS).reset_index(
        drop=True
    )
    summary = (
        triaged.groupby(["review_category", "priority_tier"], sort=True, dropna=False)
        .agg(
            review_count=("review_id", "size"),
            transaction_evidence=("transaction_evidence", "sum"),
        )
        .reset_index()
    )
    total = pd.DataFrame(
        [
            {
                "review_category": "total",
                "priority_tier": "all",
                "review_count": len(triaged),
                "transaction_evidence": triaged["transaction_evidence"].sum(),
            }
        ]
    )
    summary = pd.concat([summary, total], ignore_index=True).reindex(
        columns=_SUMMARY_COLUMNS
    )
    return triaged, template, summary
