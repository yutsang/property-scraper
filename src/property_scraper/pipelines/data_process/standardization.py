"""Shared pure transformations used by source-specific standardizers."""

from __future__ import annotations

import pandas as pd


def normalize_completion_years(
    frame: pd.DataFrame,
    candidate_columns: list[str],
    *,
    maximum_year: int = 2200,
) -> pd.DataFrame:
    """Coalesce completion-year candidates and derive current building age."""
    result = frame.copy()
    completion_year = pd.Series(pd.NA, index=result.index, dtype="Int64")

    for column in candidate_columns:
        if column not in result.columns:
            continue
        values = result[column]
        if pd.api.types.is_datetime64_any_dtype(values):
            years = values.dt.year
        else:
            numeric_years = pd.to_numeric(values, errors="coerce")
            parsed_dates = pd.to_datetime(
                values.where(numeric_years.isna()),
                errors="coerce",
            )
            years = numeric_years.where(numeric_years.notna(), parsed_dates.dt.year)
        valid_years = pd.to_numeric(years, errors="coerce").where(
            lambda series: series.between(1800, maximum_year)
        )
        completion_year = completion_year.fillna(valid_years.astype("Int64"))

    result["completion_year"] = completion_year
    result["age"] = (
        pd.Timestamp.now().year - completion_year
    ).clip(lower=0).where(completion_year.notna()).astype("Int64")
    return result


def select_ordered_columns(
    frame: pd.DataFrame,
    columns: list[str],
    *,
    rename: dict[str, str] | None = None,
    fill_missing: bool = False,
) -> tuple[pd.DataFrame, list[str]]:
    """Apply one ordered output schema and report unavailable fields."""
    result = frame.copy()
    missing = [column for column in columns if column not in result.columns]
    if fill_missing:
        for column in missing:
            result[column] = pd.Series(pd.NA, index=result.index)
        selected = columns
    else:
        selected = [column for column in columns if column in result.columns]

    result = result.loc[:, selected].copy()
    if rename:
        result = result.rename(columns=rename)
    return result, missing
