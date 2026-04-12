import pandas as pd

from property_scraper.pipelines.data_process.nodes import select_source_a_commercial_columns


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

    result = select_source_a_commercial_columns(frame)

    assert "supplement_candidate_source" in result.columns
    assert "supplement_review_status" in result.columns
    assert "match_origin" in result.columns
    assert "record_source" in result.columns
