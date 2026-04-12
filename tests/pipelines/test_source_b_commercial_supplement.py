from pathlib import Path

import pandas as pd

from property_scraper.pipelines.source_b_commercial.nodes import source_b_commercial_join


def test_source_b_commercial_join_applies_supplemental_master_and_tracks_match_method(
    tmp_path: Path,
) -> None:
    master_path = tmp_path / "buildings.xlsx"
    with pd.ExcelWriter(master_path, engine="openpyxl") as writer:
        pd.DataFrame(
            {
                "row_kind": ["manual_approved"],
                "domain": ["commercial"],
                "source_system": ["source_b_commercial"],
                "source_join_key": ["B123"],
                "source_name_raw": ["One Plaza"],
                "normalized_name": ["ONE PLAZA"],
                "district_code": [pd.NA],
                "district_name_en": ["Central"],
                "zone_en": [pd.NA],
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
                "manual_completion_year": ["2008"],
                "manual_management_company": [pd.NA],
                "manual_url": [pd.NA],
                "manual_notes": ["approved"],
                "manual_include": ["Y"],
                "native_type": [pd.NA],
                "native_title": [pd.NA],
                "native_total_area": [pd.NA],
            }
        ).to_excel(writer, sheet_name="source_b_commercial", index=False)

    transactions = pd.DataFrame(
        {
            "tx_id": ["tx-1"],
            "building_id": ["B123"],
            "eng_name": ["One Plaza"],
            "dist_name_en": ["Central"],
        }
    )
    building_details = pd.DataFrame(
        {
            "id": ["native-id"],
            "Building Name": ["Native Building"],
            "Completion": ["1995"],
        }
    )
    params = {
        "source_b_commercial": {
            "join": {
                "join_left_on": "building_id",
                "join_right_on": "id",
                "join_type": "left",
            }
        },
        "building_supplement": {
            "enabled": True,
            "workbook_file": str(master_path),
        },
    }

    result = source_b_commercial_join(
        transactions=transactions,
        building_details=building_details,
        params=params,
    )

    row = result.iloc[0]
    assert bool(row["has_building_match"]) is True
    assert row["building_match_method"] == "supplement_exact_key"
    assert row["matched_building_name"] == "One Plaza"
    assert str(row["Completion"]) == "2008"
    assert pd.isna(row["id"])
