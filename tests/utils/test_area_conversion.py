import numpy as np
import pandas as pd

from property_scraper.utils.area_conversion import (
    classify_retail_floor,
    derive_net_area,
    load_gross_to_net_ratios,
    normalize_region,
)


RATIOS = {
    "Overall Hong Kong": {
        "residential": 0.79,
        "office": 0.67,
        "retail_overall": 0.70,
        "retail_street_shop": 0.87,
        "industrial": 0.72,
    },
    "Hong Kong Island": {
        "residential": 0.81,
        "office": 0.68,
        "retail_overall": 0.71,
        "retail_street_shop": 0.87,
        "industrial": 0.73,
    },
}
FALLBACK_REGION = "Overall Hong Kong"


def test_normalize_region_maps_aliases_and_collapses_new_territories() -> None:
    assert normalize_region("HK Island") == "Hong Kong Island"
    assert normalize_region("Hong Kong Island") == "Hong Kong Island"
    assert normalize_region("New Territories East") == "New Territories"
    assert normalize_region("New Territories West") == "New Territories"
    assert normalize_region("Kowloon") == "Kowloon"


def test_normalize_region_returns_none_for_unknown_or_missing() -> None:
    assert normalize_region("Mars") is None
    assert normalize_region(None) is None
    assert normalize_region(np.nan) is None


def test_classify_retail_floor_ground_floor_markers() -> None:
    assert classify_retail_floor("Ground Shop") == "retail_street_shop"
    assert classify_retail_floor("G/F") == "retail_street_shop"
    assert classify_retail_floor("地下") == "retail_street_shop"
    assert classify_retail_floor("地下及閣樓") == "retail_street_shop"


def test_classify_retail_floor_defaults_to_overall_for_non_ground_and_missing() -> None:
    assert classify_retail_floor("Upstairs Shop") == "retail_overall"
    assert classify_retail_floor("Shopping Mall") == "retail_overall"
    assert classify_retail_floor("一樓") == "retail_overall"
    assert classify_retail_floor("商場") == "retail_overall"
    assert classify_retail_floor(None) == "retail_overall"
    assert classify_retail_floor(np.nan) == "retail_overall"


def test_load_gross_to_net_ratios_returns_table_and_fallback_when_enabled() -> None:
    params = {
        "area_conversion": {
            "enabled": True,
            "fallback_region": FALLBACK_REGION,
            "gross_to_net_ratios": RATIOS,
        }
    }
    result = load_gross_to_net_ratios(params)
    assert result is not None
    ratios, fallback_region = result
    assert fallback_region == FALLBACK_REGION
    assert ratios["Hong Kong Island"]["office"] == 0.68


def test_load_gross_to_net_ratios_returns_none_when_disabled_or_missing() -> None:
    assert load_gross_to_net_ratios({"area_conversion": {"enabled": False}}) is None
    assert load_gross_to_net_ratios({}) is None


def test_derive_net_area_preserves_original_when_present() -> None:
    net_area, provenance = derive_net_area(
        gross_area=pd.Series([1000.0]),
        property_category=pd.Series(["office"]),
        region=pd.Series(["Hong Kong Island"]),
        ratios=RATIOS,
        fallback_region=FALLBACK_REGION,
        existing_net_area=pd.Series([700.0]),
    )
    assert net_area.iloc[0] == 700.0
    assert provenance.iloc[0] == "original"


def test_derive_net_area_calculates_from_gfa_when_original_missing() -> None:
    net_area, provenance = derive_net_area(
        gross_area=pd.Series([1000.0]),
        property_category=pd.Series(["office"]),
        region=pd.Series(["Hong Kong Island"]),
        ratios=RATIOS,
        fallback_region=FALLBACK_REGION,
        existing_net_area=pd.Series([np.nan]),
    )
    assert net_area.iloc[0] == round(1000.0 * 0.68)
    assert provenance.iloc[0] == "calculated_from_gfa"


def test_derive_net_area_treats_zero_as_missing_for_both_gross_and_existing() -> None:
    net_area, provenance = derive_net_area(
        gross_area=pd.Series([0.0]),
        property_category=pd.Series(["office"]),
        region=pd.Series(["Hong Kong Island"]),
        ratios=RATIOS,
        fallback_region=FALLBACK_REGION,
        existing_net_area=pd.Series([0.0]),
    )
    assert pd.isna(net_area.iloc[0])
    assert provenance.iloc[0] == "unavailable"


def test_derive_net_area_marks_not_applicable_for_unmapped_category() -> None:
    net_area, provenance = derive_net_area(
        gross_area=pd.Series([1000.0]),
        property_category=pd.Series([None]),
        region=pd.Series(["Hong Kong Island"]),
        ratios=RATIOS,
        fallback_region=FALLBACK_REGION,
    )
    assert pd.isna(net_area.iloc[0])
    assert provenance.iloc[0] == "not_applicable"


def test_derive_net_area_marks_unavailable_when_gross_missing() -> None:
    net_area, provenance = derive_net_area(
        gross_area=pd.Series([np.nan]),
        property_category=pd.Series(["industrial"]),
        region=pd.Series(["Hong Kong Island"]),
        ratios=RATIOS,
        fallback_region=FALLBACK_REGION,
    )
    assert pd.isna(net_area.iloc[0])
    assert provenance.iloc[0] == "unavailable"


def test_derive_net_area_uses_fallback_region_when_unresolvable() -> None:
    net_area, provenance = derive_net_area(
        gross_area=pd.Series([1000.0]),
        property_category=pd.Series(["residential"]),
        region=pd.Series([None]),
        ratios=RATIOS,
        fallback_region=FALLBACK_REGION,
    )
    assert net_area.iloc[0] == round(1000.0 * RATIOS[FALLBACK_REGION]["residential"])
    assert provenance.iloc[0] == "calculated_from_gfa"


def test_derive_net_area_handles_mixed_batch_without_existing_net_area() -> None:
    net_area, provenance = derive_net_area(
        gross_area=pd.Series([1000.0, 2000.0, np.nan]),
        property_category=pd.Series(["retail_street_shop", None, "office"]),
        region=pd.Series(["Kowloon", "Hong Kong Island", "Hong Kong Island"]),
        ratios={
            **RATIOS,
            "Kowloon": {
                "residential": 0.79,
                "office": 0.67,
                "retail_overall": 0.73,
                "retail_street_shop": 0.87,
                "industrial": 0.71,
            },
        },
        fallback_region=FALLBACK_REGION,
    )
    assert provenance.tolist() == ["calculated_from_gfa", "not_applicable", "unavailable"]
    assert net_area.iloc[0] == round(1000.0 * 0.87)
    assert pd.isna(net_area.iloc[1])
    assert pd.isna(net_area.iloc[2])
