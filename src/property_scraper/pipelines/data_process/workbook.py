"""Pure assembly of the four transaction workbooks."""

from __future__ import annotations

import re
from typing import Any

import pandas as pd

try:
    from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
except ImportError:
    ILLEGAL_CHARACTERS_RE = re.compile(r"[\000-\010]|[\013-\014]|[\016-\037]")


def sanitize_worksheet_name(name: str) -> str:
    """Return an Excel-compatible worksheet name."""
    cleaned = re.sub(r"[/\\?*:\[\]]", "_", name).strip("'") or "Sheet"
    cleaned = cleaned[:31]
    return f"{cleaned}_data" if cleaned.lower() == "history" else cleaned


def sanitize_dataframe_content(frame: pd.DataFrame) -> pd.DataFrame:
    """Remove Excel-illegal characters and timezone information."""
    result = frame.copy()
    for column in result.columns:
        if isinstance(result[column].dtype, pd.DatetimeTZDtype):
            result[column] = result[column].dt.tz_localize(None)

    return result.map(
        lambda value: ILLEGAL_CHARACTERS_RE.sub("", value)
        if isinstance(value, str)
        else value
    )


def standardize_transaction_dates(
    frame: pd.DataFrame,
    date_column: str = "date",
) -> pd.DataFrame:
    """Parse supported transaction date formats without dropping valid DMY dates."""
    result = frame.copy()
    parsed = pd.to_datetime(result[date_column], format="%Y-%m-%d", errors="coerce")
    missing = parsed.isna() & result[date_column].notna()
    if missing.any():
        # format="mixed" forces per-element parsing: pandas otherwise infers
        # ONE format from the first entry and applies it to the whole batch,
        # silently coercing every differently-shaped date in the same batch to
        # NaT (e.g. a "...Z" UTC-suffixed row first in the batch makes a
        # plain "DD/MM/YYYY" row later in the same batch fail even though it
        # parses fine alone). utc=True + immediate tz-strip keeps both passes
        # tz-naive before the assignment: some sources stamp a trailing "Z"
        # and some don't, and assigning a tz-aware result into a tz-naive
        # `parsed` silently fails to convert (pandas raises a FutureWarning,
        # then a later .dt access breaks with "Can only use .dt accessor with
        # datetimelike values" because the column never became datetime64).
        second_pass = pd.to_datetime(
            result.loc[missing, date_column],
            errors="coerce",
            dayfirst=True,
            utc=True,
            format="mixed",
        ).dt.tz_localize(None)
        parsed.loc[missing] = second_pass
    if parsed.dt.tz is not None:
        parsed = parsed.dt.tz_localize(None)
    result["standard_date"] = parsed
    result["date"] = parsed
    return result


def split_by_date_window(
    frame: pd.DataFrame,
    start_year: int = 2020,
    split_year: int = 2024,
    end_year: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int | None]]:
    """Split one source into the stable historical and current workbook windows."""
    years = frame["standard_date"].dt.year
    valid = frame["standard_date"].notna()
    recent_mask = valid & years.ge(split_year)
    if end_year is not None:
        recent_mask &= years.le(end_year)

    historical = frame.loc[
        valid & years.ge(start_year) & years.lt(split_year)
    ].copy()
    recent = frame.loc[recent_mask].copy()
    valid_years = years.loc[valid]
    metrics: dict[str, int | None] = {
        "total": len(frame),
        "invalid_dates": int((~valid).sum()),
        "before_start": int((valid & years.lt(start_year)).sum()),
        "after_end": int(
            (valid & years.gt(end_year)).sum()
            if end_year is not None
            else 0
        ),
        "historical": len(historical),
        "recent": len(recent),
        "min_year": int(valid_years.min()) if not valid_years.empty else None,
        "max_year": int(valid_years.max()) if not valid_years.empty else None,
    }
    return historical, recent, metrics


def prepare_excel_sheet(frame: pd.DataFrame) -> pd.DataFrame:
    """Restore the public date column and apply stable Excel formatting."""
    result = frame.copy()
    result["date"] = pd.to_datetime(result["standard_date"], errors="coerce")
    if result["date"].dt.tz is not None:
        result["date"] = result["date"].dt.tz_localize(None)
    result = result.drop(columns=["standard_date"], errors="ignore")

    for column in ("flat", "streetno"):
        if column in result.columns:
            mask = (
                result[column].notna()
                & ~result[column].astype("string").str.startswith("'")
            )
            result.loc[mask, column] = "'" + result.loc[mask, column].astype("string")

    result = result.sort_values(
        by="date",
        ascending=False,
        kind="mergesort",
    ).reset_index(drop=True)
    return sanitize_dataframe_content(result)


def assemble_workbooks(
    source_frames: dict[str, pd.DataFrame],
    source_labels: dict[str, str],
    overlap_frames: dict[str, pd.DataFrame] | None = None,
    *,
    start_year: int = 2020,
    split_year: int = 2024,
    end_year: int | None = None,
) -> tuple[dict[str, dict[str, pd.DataFrame]], dict[str, dict[str, Any]]]:
    """Build all workbook sheet mappings without reading or writing files."""
    overlap_frames = overlap_frames or {}
    prepared: dict[str, tuple[pd.DataFrame, pd.DataFrame]] = {}
    metrics: dict[str, dict[str, Any]] = {}

    for source, frame in {**source_frames, **overlap_frames}.items():
        standardized = standardize_transaction_dates(frame)
        historical, recent, source_metrics = split_by_date_window(
            standardized,
            start_year=start_year,
            split_year=split_year,
            end_year=end_year,
        )
        prepared[source] = (historical, recent)
        metrics[source] = source_metrics

    def sheets(
        sources: tuple[str, str],
        overlap_source: str,
        window_index: int,
    ) -> dict[str, pd.DataFrame]:
        requested = [
            (source_labels[source], prepared[source][window_index])
            for source in sources
        ]
        if overlap_source in prepared:
            requested.append((source_labels[overlap_source], prepared[overlap_source][window_index]))
        return {
            sanitize_worksheet_name(label): prepare_excel_sheet(frame)
            for label, frame in requested
            if not frame.empty
        }

    outputs = {
        "residential_2020_2023": sheets(
            ("source_a_res", "source_b_res"),
            "overlap_res",
            0,
        ),
        "commercial_2020_2023": sheets(
            ("source_a_commercial", "source_b_commercial"),
            "overlap_com",
            0,
        ),
        "residential_2024_current": sheets(
            ("source_a_res", "source_b_res"),
            "overlap_res",
            1,
        ),
        "commercial_2024_current": sheets(
            ("source_a_commercial", "source_b_commercial"),
            "overlap_com",
            1,
        ),
    }
    return outputs, metrics
