from pathlib import Path

import pandas as pd

from property_scraper.pipelines.source_a_commercial.nodes import (
    join_transaction_with_building_details,
)


def test_join_transaction_with_building_details_uses_supplemental_master_for_unmatched_rows(
    tmp_path: Path,
) -> None:
    master_path = tmp_path / "buildings.xlsx"
    with pd.ExcelWriter(master_path, engine="openpyxl") as writer:
        pd.DataFrame(
            {
                "row_kind": ["manual_approved"],
                "domain": ["commercial"],
                "source_system": ["source_a_commercial"],
                "source_join_key": [None],
                "source_name_raw": ["One Plaza"],
                "normalized_name": ["ONE PLAZA"],
                "district_code": [pd.NA],
                "district_name_en": ["Central"],
                "zone_en": ["HK Island"],
                "occurrence_count": [1],
                "candidate_building_name": [pd.NA],
                "candidate_source": [pd.NA],
                "native_address": [pd.NA],
                "native_completion_year": [pd.NA],
                "native_management_company": [pd.NA],
                "native_url": [pd.NA],
                "native_detail_source": [pd.NA],
                "manual_canonical_name": ["One Plaza"],
                "manual_address": [pd.NA],
                "manual_completion_year": ["2001"],
                "manual_management_company": [pd.NA],
                "manual_url": [pd.NA],
                "manual_notes": ["approved"],
                "manual_include": ["Y"],
                "native_grade": [pd.NA],
                "native_usage": [pd.NA],
                "native_property_type": [pd.NA],
                "native_developers": [pd.NA],
            }
        ).to_excel(writer, sheet_name="source_a_commercial", index=False)

    transaction_data = pd.DataFrame(
        {
            "id": ["tx-1"],
            "propertyId": ["missing"],
            "propertyNameEn": ["One Plaza"],
            "districtNameEn": ["Central"],
            "zoneEn": ["HK Island"],
        }
    )
    building_details = pd.DataFrame(
        {
            "property_id": ["known"],
            "building_name": ["Another Building"],
            "completion_year": ["1999"],
        }
    )
    params = {
        "source_a_commercial": {
            "join": {
                "use_exact_id_first": True,
                "fallback_to_fuzzy": False,
                "transaction_name_col": "propertyNameEn",
                "building_name_col": "building_name",
                "left_on": "propertyId",
                "right_on": "property_id",
            }
        },
        "building_supplement": {
            "enabled": True,
            "workbook_file": str(master_path),
        },
    }

    result = join_transaction_with_building_details(
        transaction_data=transaction_data,
        building_details=building_details,
        params=params,
    )

    row = result.iloc[0]
    assert row["_match_method"] == "supplement_reviewed_name"
    assert row["matched_building_name"] == "One Plaza"
    assert str(row["completion_year"]) == "2001"
