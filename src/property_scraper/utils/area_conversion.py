from __future__ import annotations

import logging
from typing import Any, Optional

import numpy as np
import pandas as pd


logger = logging.getLogger(__name__)

PROPERTY_CATEGORIES = [
    "residential",
    "office",
    "retail_overall",
    "retail_street_shop",
    "industrial",
]

_REGION_ALIASES = {
    "HK ISLAND": "Hong Kong Island",
    "HONG KONG ISLAND": "Hong Kong Island",
    "KOWLOON": "Kowloon",
    "NEW TERRITORIES": "New Territories",
    "NEW TERRITORIES EAST": "New Territories",
    "NEW TERRITORIES WEST": "New Territories",
}

_GROUND_FLOOR_EN_MARKERS = ("GROUND", "G/F", "GF")


def normalize_region(value: Any) -> Optional[str]:
    """Map a source-specific region label to one of the canonical ratio-table regions."""
    if value is None or pd.isna(value):
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    return _REGION_ALIASES.get(text)


def classify_retail_floor(floor_value: Any) -> str:
    """Classify a retail unit as a ground-level street shop or general retail.

    No source carries an explicit "street shop" flag. This uses the `floor` text as a
    proxy: ground-floor markers (English "Ground"/"G/F"; Chinese "地下") count as a
    street shop, everything else (upper floors, malls, ambiguous/missing text) falls
    back to the more conservative retail_overall ratio.
    """
    if floor_value is None or pd.isna(floor_value):
        return "retail_overall"
    text = str(floor_value).strip()
    if not text:
        return "retail_overall"
    if "地下" in text:
        return "retail_street_shop"
    upper = text.upper()
    if any(marker in upper for marker in _GROUND_FLOOR_EN_MARKERS):
        return "retail_street_shop"
    return "retail_overall"


def load_gross_to_net_ratios(
    params: dict[str, Any],
) -> Optional[tuple[dict[str, dict[str, float]], str]]:
    """Load the GFA->NFA ratio table and fallback region from pipeline parameters.

    Returns None when the feature is disabled (`area_conversion.enabled: false`),
    mirroring the enabled-flag pattern used by building_supplement's supplemental
    master loader.
    """
    config = params.get("area_conversion", {})
    if not config.get("enabled", False):
        return None

    ratios = config.get("gross_to_net_ratios") or {}
    fallback_region = config.get("fallback_region")
    if not ratios or not fallback_region:
        logger.info("area_conversion enabled but ratio table or fallback_region missing")
        return None

    return ratios, fallback_region


def derive_net_area(
    *,
    gross_area: pd.Series,
    property_category: pd.Series,
    region: pd.Series,
    ratios: dict[str, dict[str, float]],
    fallback_region: str,
    existing_net_area: Optional[pd.Series] = None,
) -> tuple[pd.Series, pd.Series]:
    """Fill missing NFA from GFA using region/property-type ratios.

    All input Series must share the same index. `property_category` entries of None
    (or NaN) mean "no ratio category applies" (e.g. Carpark) and are never calculated.
    A gross/net area value of exactly 0 is treated as missing, matching the 0-as-missing
    sentinel convention some sources use instead of NaN.

    Returns (net_area, provenance) where provenance is one of:
    'original' | 'calculated_from_gfa' | 'unavailable' | 'not_applicable'.
    """
    index = gross_area.index
    gross = pd.to_numeric(gross_area, errors="coerce").mask(lambda s: s == 0)

    if existing_net_area is not None:
        existing = pd.to_numeric(existing_net_area, errors="coerce").mask(lambda s: s == 0)
    else:
        existing = pd.Series(np.nan, index=index)

    resolved_region = region.map(
        lambda value: value if value in ratios else fallback_region
    )

    def _ratio_for(category: Any, region_key: Any) -> Optional[float]:
        if category is None or (isinstance(category, float) and pd.isna(category)):
            return None
        return ratios.get(region_key, {}).get(category)

    ratio_values = pd.Series(
        [_ratio_for(c, r) for c, r in zip(property_category, resolved_region)],
        index=index,
        dtype="float64",
    )

    has_category = property_category.map(lambda v: v is not None and not pd.isna(v))
    has_gross = gross.notna()
    has_ratio = ratio_values.notna()
    can_calculate = has_category & has_gross & has_ratio
    has_existing = existing.notna()

    net_area = pd.Series(np.nan, index=index, dtype="float64")
    provenance = pd.Series("unavailable", index=index, dtype="string")

    provenance = provenance.mask(~has_category, "not_applicable")
    net_area = net_area.mask(can_calculate, (gross * ratio_values).round())
    provenance = provenance.mask(can_calculate, "calculated_from_gfa")
    net_area = net_area.mask(has_existing, existing)
    provenance = provenance.mask(has_existing, "original")

    return net_area, provenance
