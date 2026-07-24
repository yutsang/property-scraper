import numpy as np
import pandas as pd

from property_scraper.pipelines.data_process.nodes import (
    select_source_a_commercial_columns,
    select_source_a_res_columns,
    select_source_b_commercial_columns,
    select_source_b_res_columns,
)


AREA_CONVERSION_PARAMS = {
    "area_conversion": {
        "enabled": True,
        "fallback_region": "Overall Hong Kong",
        "gross_to_net_ratios": {
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
        },
    }
}


def test_select_source_a_commercial_columns_keeps_supplement_provenance_fields() -> None:
    frame = pd.DataFrame(
        {
            "transactionDate": ["2024-01-01"],
            "zoneEn": ["HK Island"],
            "districtNameEn": ["Central"],
            "propertyUsageDisplayName": ["Office"],
            "grade": ["A"],
            "propertyNameEn": ["One Plaza"],
            "propertyNameCn": ["第一廣場"],
            "floor": ["10/F"],
            "unit": ["A"],
            "transactionType": ["sale"],
            "transactionArea": [1000],
            "price": [10000000],
            "avgPrice": [10000],
            "full_address": ["1 Example Road"],
            "completion_year": ["2000"],
            "age": [24],
            "source_url": ["https://example.com"],
            "management_company": ["ABC"],
            "developers": ["XYZ"],
            "carpark": ["Y"],
            "sourceDisplayName": ["Source A"],
            "Datasource": ["source_a_commercial"],
            "id": ["tx-1"],
            "matched_building_name": ["One Plaza"],
            "match_score": [100.0],
            "_match_method": ["supplement_reviewed_name"],
            "_match_score": [100.0],
            "supplement_candidate_source": ["source_b_commercial_primary"],
            "supplement_review_status": ["approved"],
            "match_origin": ["manual"],
            "record_source": ["excel_source_tab"],
        }
    )

    result = select_source_a_commercial_columns(frame, {"area_conversion": {"enabled": False}})

    assert "supplement_candidate_source" in result.columns
    assert "supplement_review_status" in result.columns
    assert "match_origin" in result.columns
    assert "record_source" in result.columns


def test_select_source_a_commercial_columns_derives_net_area_from_gfa() -> None:
    frame = pd.DataFrame(
        {
            "transactionDate": ["2024-01-01", "2024-01-02"],
            "zoneEn": ["HK Island", "HK Island"],
            "propertyUsageDisplayName": ["Commercial", "Retail"],
            "floor": ["10/F", "Ground Shop"],
            "transactionArea": [1000.0, 500.0],
            "price": [10000000, 5000000],
        }
    )

    result = select_source_a_commercial_columns(frame, AREA_CONVERSION_PARAMS)

    assert result.loc[0, "net_area"] == round(1000.0 * 0.68)  # office ratio
    assert result.loc[0, "area_provenance"] == "calculated_from_gfa"
    assert result.loc[1, "net_area"] == round(500.0 * 0.87)  # ground-floor retail street shop
    assert result.loc[1, "area_provenance"] == "calculated_from_gfa"


def test_select_source_b_commercial_columns_derives_net_area_from_gfa() -> None:
    frame = pd.DataFrame(
        {
            "tx_date": ["2024-01-01", "2024-01-02"],
            "zoneEn": ["HK Island", "HK Island"],
            "ics_type": ["Office", "Retail"],
            "floor": ["10/F", "地下"],
            "area": [1000, 500],
        }
    )

    result = select_source_b_commercial_columns(frame, AREA_CONVERSION_PARAMS)

    assert result.loc[0, "net_area"] == round(1000.0 * 0.68)
    assert result.loc[0, "area_provenance"] == "calculated_from_gfa"
    assert result.loc[1, "net_area"] == round(500.0 * 0.87)  # 地下 = ground floor street shop
    assert result.loc[1, "area_provenance"] == "calculated_from_gfa"


def test_select_source_a_res_columns_fills_area_from_gfa_when_missing() -> None:
    frame = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02"],
            "region": ["Hong Kong Island", "Hong Kong Island"],
            "property_type": ["residential", "residential"],
            "area": [np.nan, 800.0],
            "g_area": [1000.0, 1200.0],
        }
    )

    result = select_source_a_res_columns(frame, AREA_CONVERSION_PARAMS)

    assert result.loc[0, "area"] == round(1000.0 * 0.81)
    assert result.loc[0, "area_provenance"] == "calculated_from_gfa"
    assert result.loc[1, "area"] == 800.0
    assert result.loc[1, "area_provenance"] == "original"


def test_select_source_b_res_columns_fills_net_area_from_gfa_when_missing() -> None:
    frame = pd.DataFrame(
        {
            "tx_date": ["2024-01-01", "2024-01-02"],
            "region_name_trans": ["Hong Kong Island", "Hong Kong Island"],
            "net_area": [0, 500],
            "area": [1000, 700],
        }
    )

    result = select_source_b_res_columns(frame, AREA_CONVERSION_PARAMS)

    assert result.loc[0, "net_area"] == round(1000.0 * 0.81)
    assert result.loc[0, "area_provenance"] == "calculated_from_gfa"
    assert result.loc[1, "net_area"] == 500.0
    assert result.loc[1, "area_provenance"] == "original"
