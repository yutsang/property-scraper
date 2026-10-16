"""Fail-closed publication checks for primary transaction datasets."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from .workbook import standardize_transaction_dates

logger = logging.getLogger(__name__)

_COVERAGE_FLOOR = 0.5
_COVERAGE_BASELINE_MONTHS = 12


def find_coverage_gaps(
    frame: pd.DataFrame, floor: float = _COVERAGE_FLOOR
) -> list[dict[str, Any]]:
    """Return recent months whose volume collapsed against their trailing median.

    A scrape that silently loses areas shows up here as a run of thin months.
    The 2026-02..05 hole in Centaline residential went unnoticed for months
    because nothing compared a month against the ones before it.
    """
    dates = standardize_transaction_dates(frame)["standard_date"].dropna()
    if dates.empty:
        return []
    monthly = dates.dt.to_period("M").value_counts().sort_index()
    # The final month is usually still in progress, so it is never a gap.
    gaps: list[dict[str, Any]] = []
    for period in monthly.index[-13:-1]:
        prior = monthly[monthly.index < period].tail(_COVERAGE_BASELINE_MONTHS)
        if len(prior) < _COVERAGE_BASELINE_MONTHS:
            continue
        baseline = float(prior.median())
        observed = int(monthly[period])
        if baseline > 0 and observed < baseline * floor:
            gaps.append(
                {
                    "month": str(period),
                    "rows": observed,
                    "expected": round(baseline),
                    "ratio": round(observed / baseline, 3),
                }
            )
    return gaps


def validate_publication_inputs(
    source_frames: dict[str, pd.DataFrame],
) -> dict[str, dict[str, Any]]:
    """Validate invariants that must hold before writing public workbooks."""
    report: dict[str, dict[str, Any]] = {}
    failures: list[str] = []

    for source, frame in source_frames.items():
        if "date" not in frame.columns:
            failures.append(f"{source}: missing required date column")
            continue
        standardized = standardize_transaction_dates(frame)
        invalid_dates = int(standardized["standard_date"].isna().sum())
        completion_year = pd.to_numeric(
            frame.get(
                "completion_year",
                pd.Series(pd.NA, index=frame.index),
            ),
            errors="coerce",
        )
        invalid_completion_years = int(
            (
                completion_year.notna()
                & ~completion_year.between(1800, 2200)
            ).sum()
        )
        coverage_gaps = find_coverage_gaps(frame)
        report[source] = {
            "rows": len(frame),
            "invalid_dates": invalid_dates,
            "invalid_completion_years": invalid_completion_years,
            "coverage_gaps": coverage_gaps,
            "status": "pass"
            if invalid_dates == 0 and invalid_completion_years == 0
            else "failed",
        }
        for gap in coverage_gaps:
            logger.warning(
                "⚠️ %s: %s has %s rows, %.0f%% of the trailing median %s",
                source,
                gap["month"],
                gap["rows"],
                gap["ratio"] * 100,
                gap["expected"],
            )
        if invalid_dates:
            failures.append(f"{source}: {invalid_dates} invalid dates")
        if invalid_completion_years:
            failures.append(
                f"{source}: {invalid_completion_years} invalid completion years"
            )

    if failures:
        raise ValueError("Publication quality gate failed: " + "; ".join(failures))
    return report


def validate_primary_for_publication(
    source_a_res: pd.DataFrame,
    source_a_commercial: pd.DataFrame,
    source_b_res: pd.DataFrame,
    source_b_commercial: pd.DataFrame,
) -> dict[str, dict[str, Any]]:
    """Kedro node interface for the four primary source frames."""
    return validate_publication_inputs(
        {
            "source_a_res": source_a_res,
            "source_a_commercial": source_a_commercial,
            "source_b_res": source_b_res,
            "source_b_commercial": source_b_commercial,
        }
    )
