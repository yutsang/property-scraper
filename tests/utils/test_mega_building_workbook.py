from pathlib import Path

import pandas as pd

from property_scraper.utils.building_supplement import (
    build_mega_building_workbook,
    build_consolidated_commercial_master,
    load_supplemental_building_master,
    merge_manual_rows_into_source_buildings,
    write_mega_building_workbook,
)


def test_build_mega_building_workbook_creates_four_combined_source_tabs() -> None:
    workbook_sheets, supplement_cache, consolidated_commercial = build_mega_building_workbook(
        source_a_commercial=pd.DataFrame(
            {
                "propertyNameEn": ["One Plaza", "One Plaza"],
                "_match_method": ["unmatched", "unmatched"],
                "districtNameEn": ["Central", "Central"],
                "zoneEn": ["HK Island", "HK Island"],
            }
        ),
        source_b_commercial_base=pd.DataFrame(
            {
                "eng_name": ["Source B House"],
                "building_id": ["B1"],
                "has_building_match": [False],
                "dist_name_en": ["Central"],
                "dist_code": ["CEN"],
            }
        ),
        source_b_commercial_primary=pd.DataFrame({"eng_name": ["ONE PLAZA", "MIDLAND HOUSE"]}),
        source_a_res=pd.DataFrame(
            {
                "Name": ["Res Estate"],
                "building_code": [pd.NA],
                "district": ["Sha Tin"],
                "region": ["New Territories"],
            }
        ),
        source_b_res=pd.DataFrame(
            {
                "estate": ["Res Court"],
                "building": [pd.NA],
                "district": ["Tsuen Wan"],
                "region_name_trans": ["New Territories"],
            }
        ),
        source_a_commercial_buildings=pd.DataFrame(
            {
                "property_id": ["P1"],
                "building_name": ["Native OIR"],
                "district": ["Central"],
                "zone": ["HK Island"],
                "full_address": ["1 Example Road"],
                "completion_year": ["2001"],
                "management_company": ["ABC"],
                "source_url": ["https://oir.example"],
            }
        ),
        source_b_commercial_buildings=pd.DataFrame(
            {
                "id": ["M1"],
                "Building Name": ["Native ICI"],
                "Completion": ["1999"],
                "Management Company": ["XYZ"],
                "URL": ["https://ici.example"],
            }
        ),
        source_a_res_buildings=pd.DataFrame(
            {
                "estate_code": ["E1"],
                "Name": ["Native Estate"],
                "District": ["Sha Tin"],
                "Region": ["New Territories"],
                "estate_detailed_address": ["2 Estate Road"],
                "occupation_permit": ["2005"],
                "developer": ["Dev A"],
                "Link": ["https://res.example"],
            }
        ),
        source_b_res_buildings=pd.DataFrame(
            {
                "id": ["R1"],
                "name": ["Native Source B Estate"],
                "district_name": ["Tsuen Wan"],
                "region_name": ["New Territories"],
                "location": ["3 Source B Road"],
                "first_op_date": ["2010"],
                "developer_name": ["Dev B"],
                "url_desc": ["https://midres.example"],
            }
        ),
        workbook_path=Path("/tmp/nonexistent_buildings.xlsx"),
        include_limit=0,
        source_c_frames=[],
    )

    assert set(workbook_sheets) == {
        "source_a_res",
        "source_a_commercial",
        "source_b_res",
        "source_b_commercial",
    }
    assert supplement_cache.empty

    source_a_commercial_tab = workbook_sheets["source_a_commercial"]
    assert set(source_a_commercial_tab["row_kind"]) == {"native", "unmatched_candidate"}
    one_plaza = source_a_commercial_tab.loc[
        source_a_commercial_tab["source_name_raw"] == "One Plaza"
    ].iloc[0]
    assert one_plaza["occurrence_count"] == 2

    source_a_res_tab = workbook_sheets["source_a_res"]
    assert "Res Estate" in set(source_a_res_tab["source_name_raw"])
    assert "Native Estate" in set(source_a_res_tab["source_name_raw"])

    assert "Native OIR" in set(consolidated_commercial["canonical_building_name"])
    assert "Native ICI" in set(consolidated_commercial["canonical_building_name"])


def test_build_mega_building_workbook_preserves_manual_rows_and_exports_marked_cache(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "buildings.xlsx"
    with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
        pd.DataFrame(
            {
                "row_kind": ["unmatched_candidate", "unmatched_candidate"],
                "domain": ["commercial", "commercial"],
                "source_system": ["source_a_commercial", "source_a_commercial"],
                "source_join_key": [pd.NA, pd.NA],
                "source_name_raw": ["Approved Plaza", "Old Tower"],
                "normalized_name": ["APPROVED PLAZA", "OLD TOWER"],
                "district_code": [pd.NA, pd.NA],
                "district_name_en": ["Central", "Central"],
                "zone_en": ["HK Island", "HK Island"],
                "occurrence_count": [5, 1],
                "native_address": [pd.NA, pd.NA],
                "native_completion_year": [pd.NA, pd.NA],
                "native_management_company": [pd.NA, pd.NA],
                "native_url": [pd.NA, pd.NA],
                "native_detail_source": [pd.NA, pd.NA],
                "manual_canonical_name": ["Approved Plaza", pd.NA],
                "manual_address": ["1 Manual Road", pd.NA],
                "manual_completion_year": ["2008", pd.NA],
                "manual_management_company": ["Manual Co", pd.NA],
                "manual_url": ["https://manual.example", pd.NA],
                "manual_notes": ["approved", "keep me"],
                "manual_include": ["Y", pd.NA],
            }
        ).to_excel(writer, sheet_name="source_a_commercial", index=False)

    workbook_sheets, supplement_cache, _ = build_mega_building_workbook(
        source_a_commercial=pd.DataFrame(
            {
                "propertyNameEn": ["New Tower"],
                "_match_method": ["unmatched"],
                "districtNameEn": ["Central"],
                "zoneEn": ["HK Island"],
            }
        ),
        source_b_commercial_base=pd.DataFrame(),
        source_b_commercial_primary=pd.DataFrame(),
        source_a_res=pd.DataFrame(),
        source_b_res=pd.DataFrame(),
        source_a_commercial_buildings=pd.DataFrame(),
        source_b_commercial_buildings=pd.DataFrame(),
        source_a_res_buildings=pd.DataFrame(),
        source_b_res_buildings=pd.DataFrame(),
        workbook_path=workbook_path,
        include_limit=0,
        source_c_frames=[],
    )

    source_a_commercial_tab = workbook_sheets["source_a_commercial"]
    assert "Old Tower" in set(source_a_commercial_tab["source_name_raw"])
    assert "New Tower" in set(source_a_commercial_tab["source_name_raw"])

    approved_row = source_a_commercial_tab.loc[
        source_a_commercial_tab["source_name_raw"] == "Approved Plaza"
    ].iloc[0]
    assert approved_row["row_kind"] == "manual_approved"

    assert set(supplement_cache["canonical_building_name"]) == {"Approved Plaza"}
    cache_row = supplement_cache.iloc[0]
    assert cache_row["source_system"] == "source_a_commercial"
    assert cache_row["record_source"] == "mega_workbook"


def test_build_mega_building_workbook_keeps_existing_rows_when_no_fresh_candidates(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "buildings.xlsx"
    with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
        pd.DataFrame(
            {
                "row_kind": ["unmatched_candidate"],
                "domain": ["commercial"],
                "source_system": ["source_a_commercial"],
                "source_join_key": [pd.NA],
                "source_name_raw": ["Carry Forward Tower"],
                "normalized_name": ["CARRY FORWARD TOWER"],
                "district_code": [pd.NA],
                "district_name_en": ["Central"],
                "zone_en": ["HK Island"],
                "occurrence_count": [3],
                "candidate_building_name": [pd.NA],
                "candidate_source": [pd.NA],
                "native_address": [pd.NA],
                "native_completion_year": [pd.NA],
                "native_management_company": [pd.NA],
                "native_url": [pd.NA],
                "native_detail_source": [pd.NA],
                "manual_canonical_name": [pd.NA],
                "manual_address": [pd.NA],
                "manual_completion_year": [pd.NA],
                "manual_management_company": [pd.NA],
                "manual_url": [pd.NA],
                "manual_notes": ["keep"],
                "manual_include": [pd.NA],
                "native_grade": [pd.NA],
                "native_usage": [pd.NA],
                "native_property_type": [pd.NA],
                "native_developers": [pd.NA],
            }
        ).to_excel(writer, sheet_name="source_a_commercial", index=False)

    workbook_sheets, supplement_cache, _ = build_mega_building_workbook(
        source_a_commercial=pd.DataFrame(),
        source_b_commercial_base=pd.DataFrame(),
        source_b_commercial_primary=pd.DataFrame(),
        source_a_res=pd.DataFrame(),
        source_b_res=pd.DataFrame(),
        source_a_commercial_buildings=pd.DataFrame(),
        source_b_commercial_buildings=pd.DataFrame(),
        source_a_res_buildings=pd.DataFrame(),
        source_b_res_buildings=pd.DataFrame(),
        workbook_path=workbook_path,
        include_limit=0,
        source_c_frames=[],
    )

    assert "Carry Forward Tower" in set(workbook_sheets["source_a_commercial"]["source_name_raw"])
    assert supplement_cache.empty


def test_write_mega_building_workbook_sanitizes_excel_illegal_characters(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "buildings.xlsx"

    write_mega_building_workbook(
        workbook_path,
        {
            "source_a_commercial": pd.DataFrame(
                {
                    "row_kind": ["unmatched_candidate"],
                    "domain": ["commercial"],
                    "source_system": ["source_a_commercial"],
                    "source_join_key": [pd.NA],
                    "source_name_raw": ["Bad\x1aName"],
                    "normalized_name": ["BAD NAME"],
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
                    "manual_canonical_name": [pd.NA],
                    "manual_address": [pd.NA],
                    "manual_completion_year": [pd.NA],
                    "manual_management_company": [pd.NA],
                    "manual_url": [pd.NA],
                    "manual_notes": [pd.NA],
                    "manual_include": [pd.NA],
                    "native_grade": [pd.NA],
                    "native_usage": [pd.NA],
                    "native_property_type": [pd.NA],
                    "native_developers": [pd.NA],
                }
            )
        },
    )

    sheet = pd.read_excel(workbook_path, sheet_name="source_a_commercial")
    assert sheet.loc[0, "source_name_raw"] == "BadName"


def test_write_mega_building_workbook_normalizes_timezone_datetimes(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "buildings.xlsx"

    write_mega_building_workbook(
        workbook_path,
        {
            "source_a_commercial": pd.DataFrame(
                {
                    "row_kind": ["unmatched_candidate"],
                    "domain": ["commercial"],
                    "source_system": ["source_a_commercial"],
                    "source_join_key": [pd.NA],
                    "source_name_raw": ["Timed Tower"],
                    "normalized_name": ["TIMED TOWER"],
                    "district_code": [pd.NA],
                    "district_name_en": ["Central"],
                    "zone_en": ["HK Island"],
                    "occurrence_count": [1],
                    "candidate_building_name": [pd.NA],
                    "candidate_source": [pd.NA],
                    "native_address": [pd.NA],
                    "native_completion_year": [pd.Timestamp("2024-01-01T12:00:00Z")],
                    "native_management_company": [pd.NA],
                    "native_url": [pd.NA],
                    "native_detail_source": [pd.NA],
                    "manual_canonical_name": [pd.NA],
                    "manual_address": [pd.NA],
                    "manual_completion_year": [pd.NA],
                    "manual_management_company": [pd.NA],
                    "manual_url": [pd.NA],
                    "manual_notes": [pd.NA],
                    "manual_include": [pd.NA],
                    "native_grade": [pd.NA],
                    "native_usage": [pd.NA],
                    "native_property_type": [pd.NA],
                    "native_developers": [pd.NA],
                }
            )
        },
    )

    sheet = pd.read_excel(workbook_path, sheet_name="source_a_commercial")
    assert not sheet.empty


def test_load_supplemental_building_master_reads_marked_rows_from_mega_workbook(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "buildings.xlsx"
    write_mega_building_workbook(
        workbook_path,
        {
            "source_a_commercial": pd.DataFrame(
                {
                    "row_kind": ["manual_approved"],
                    "domain": ["commercial"],
                    "source_system": ["source_a_commercial"],
                    "source_join_key": [pd.NA],
                    "source_name_raw": ["Workbook Plaza"],
                    "normalized_name": ["WORKBOOK PLAZA"],
                    "district_code": [pd.NA],
                    "district_name_en": ["Central"],
                    "zone_en": ["HK Island"],
                    "occurrence_count": [2],
                    "candidate_building_name": [pd.NA],
                    "candidate_source": [pd.NA],
                    "native_address": [pd.NA],
                    "native_completion_year": [pd.NA],
                    "native_management_company": [pd.NA],
                    "native_url": [pd.NA],
                    "native_detail_source": [pd.NA],
                    "manual_canonical_name": ["Workbook Plaza"],
                    "manual_address": ["1 Workbook Road"],
                    "manual_completion_year": ["2001"],
                    "manual_management_company": ["Workbook Co"],
                    "manual_url": ["https://workbook.example"],
                    "manual_notes": ["approved"],
                    "manual_include": ["Y"],
                    "native_grade": [pd.NA],
                    "native_usage": [pd.NA],
                    "native_property_type": [pd.NA],
                    "native_developers": [pd.NA],
                }
            )
        },
    )

    master = load_supplemental_building_master(
        {
            "building_supplement": {
                "enabled": True,
                "master_file": str(workbook_path),
            }
        },
        source_system="source_a_commercial",
    )

    assert set(master["canonical_building_name"]) == {"Workbook Plaza"}


def test_load_supplemental_building_master_uses_workbook_file_when_master_file_missing(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "buildings.xlsx"
    write_mega_building_workbook(
        workbook_path,
        {
            "source_a_commercial": pd.DataFrame(
                {
                    "row_kind": ["manual_approved"],
                    "domain": ["commercial"],
                    "source_system": ["source_a_commercial"],
                    "source_join_key": [pd.NA],
                    "source_name_raw": ["Workbook Plaza"],
                    "normalized_name": ["WORKBOOK PLAZA"],
                    "district_code": [pd.NA],
                    "district_name_en": ["Central"],
                    "zone_en": ["HK Island"],
                    "occurrence_count": [2],
                    "candidate_building_name": [pd.NA],
                    "candidate_source": [pd.NA],
                    "native_address": [pd.NA],
                    "native_completion_year": [pd.NA],
                    "native_management_company": [pd.NA],
                    "native_url": [pd.NA],
                    "native_detail_source": [pd.NA],
                    "manual_canonical_name": ["Workbook Plaza"],
                    "manual_address": ["1 Workbook Road"],
                    "manual_completion_year": ["2001"],
                    "manual_management_company": ["Workbook Co"],
                    "manual_url": ["https://workbook.example"],
                    "manual_notes": ["approved"],
                    "manual_include": ["Y"],
                    "native_grade": [pd.NA],
                    "native_usage": [pd.NA],
                    "native_property_type": [pd.NA],
                    "native_developers": [pd.NA],
                }
            )
        },
    )

    master = load_supplemental_building_master(
        {
            "building_supplement": {
                "enabled": True,
                "workbook_file": str(workbook_path),
            }
        },
        source_system="source_a_commercial",
    )

    assert set(master["canonical_building_name"]) == {"Workbook Plaza"}


def test_merge_manual_rows_into_source_buildings_replaces_stale_manual_rows() -> None:
    approved_master = pd.DataFrame(
        {
            "domain": ["commercial", "residential"],
            "source_system": ["source_a_commercial", "source_b_res"],
            "source_join_key": ["P-MANUAL", "R-MANUAL"],
            "source_name_raw": ["Workbook Plaza", "Workbook Court"],
            "normalized_name": ["WORKBOOK PLAZA", "WORKBOOK COURT"],
            "district_code": [pd.NA, pd.NA],
            "district_name_en": ["Central", "Tsuen Wan"],
            "zone_en": ["HK Island", "New Territories"],
            "candidate_building_name": [pd.NA, pd.NA],
            "candidate_source": [pd.NA, pd.NA],
            "review_status": ["approved", "approved"],
            "review_notes": ["approved", "approved"],
            "canonical_building_name": ["Workbook Plaza", "Workbook Court"],
            "address": ["1 Workbook Road", "8 Workbook Street"],
            "completion_year": ["2001", "2012"],
            "management_company": ["Workbook Co", pd.NA],
            "url": ["https://workbook.example", "https://court.example"],
            "match_origin": ["manual", "manual"],
            "record_source": ["mega_workbook", "mega_workbook"],
        }
    )

    updated_frames = merge_manual_rows_into_source_buildings(
        approved_master=approved_master,
        source_a_commercial_buildings=pd.DataFrame(
            {
                "property_id": ["P-NATIVE", "P-OLD"],
                "building_name": ["Native Tower", "Old Manual Plaza"],
                "district": ["Central", "Central"],
                "zone": ["HK Island", "HK Island"],
                "full_address": ["1 Native Road", "Old Address"],
                "completion_year": ["1999", "1998"],
                "management_company": ["Native Co", "Old Co"],
                "source_url": ["https://native.example", "https://old.example"],
                "record_source": ["scraped", "manual_workbook"],
                "match_origin": [pd.NA, "manual"],
                "is_manual_record": [False, True],
            }
        ),
        source_b_commercial_buildings=pd.DataFrame(),
        source_a_res_buildings=pd.DataFrame(),
        source_b_res_buildings=pd.DataFrame(
            {
                "id": ["R-NATIVE", "R-OLD"],
                "name": ["Native Court", "Old Workbook Court"],
                "district_name": ["Tsuen Wan", "Tsuen Wan"],
                "region_name": ["New Territories", "New Territories"],
                "location": ["7 Native Street", "Old Workbook Street"],
                "first_op_date": ["2010", "2009"],
                "url_desc": ["https://native-court.example", "https://old-court.example"],
                "record_source": ["scraped", "manual_workbook"],
                "match_origin": [pd.NA, "manual"],
                "is_manual_record": [False, True],
            }
        ),
    )

    source_a_commercial = updated_frames["source_a_commercial"]
    assert "Native Tower" in set(source_a_commercial["building_name"])
    assert "Old Manual Plaza" not in set(source_a_commercial["building_name"])
    manual_oir = source_a_commercial.loc[source_a_commercial["building_name"] == "Workbook Plaza"].iloc[0]
    assert manual_oir["property_id"] == "P-MANUAL"
    assert manual_oir["record_source"] == "manual_workbook"
    assert bool(manual_oir["is_manual_record"]) is True

    source_b_res = updated_frames["source_b_res"]
    assert "Native Court" in set(source_b_res["name"])
    assert "Old Workbook Court" not in set(source_b_res["name"])
    manual_source_b_res = source_b_res.loc[source_b_res["name"] == "Workbook Court"].iloc[0]
    assert manual_source_b_res["id"] == "R-MANUAL"
    assert manual_source_b_res["record_source"] == "manual_workbook"
    assert bool(manual_source_b_res["is_manual_record"]) is True


def test_build_mega_building_workbook_ignores_manual_writeback_rows_in_native_inputs(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "buildings.xlsx"
    write_mega_building_workbook(
        workbook_path,
        {
            "source_a_commercial": pd.DataFrame(
                {
                    "row_kind": ["manual_approved"],
                    "domain": ["commercial"],
                    "source_system": ["source_a_commercial"],
                    "source_join_key": ["P-MANUAL"],
                    "source_name_raw": ["Workbook Plaza"],
                    "normalized_name": ["WORKBOOK PLAZA"],
                    "district_code": [pd.NA],
                    "district_name_en": ["Central"],
                    "zone_en": ["HK Island"],
                    "occurrence_count": [2],
                    "candidate_building_name": [pd.NA],
                    "candidate_source": [pd.NA],
                    "native_address": [pd.NA],
                    "native_completion_year": [pd.NA],
                    "native_management_company": [pd.NA],
                    "native_url": [pd.NA],
                    "native_detail_source": [pd.NA],
                    "manual_canonical_name": ["Workbook Plaza"],
                    "manual_address": ["1 Workbook Road"],
                    "manual_completion_year": ["2001"],
                    "manual_management_company": ["Workbook Co"],
                    "manual_url": ["https://workbook.example"],
                    "manual_notes": ["approved"],
                    "manual_include": ["Y"],
                    "native_grade": [pd.NA],
                    "native_usage": [pd.NA],
                    "native_property_type": [pd.NA],
                    "native_developers": [pd.NA],
                }
            )
        },
    )

    workbook_sheets, _, _ = build_mega_building_workbook(
        source_a_commercial=pd.DataFrame(),
        source_b_commercial_base=pd.DataFrame(),
        source_b_commercial_primary=pd.DataFrame(),
        source_a_res=pd.DataFrame(),
        source_b_res=pd.DataFrame(),
        source_a_commercial_buildings=pd.DataFrame(
            {
                "property_id": ["P-MANUAL"],
                "building_name": ["Workbook Plaza"],
                "district": ["Central"],
                "zone": ["HK Island"],
                "full_address": ["1 Workbook Road"],
                "completion_year": ["2001"],
                "management_company": ["Workbook Co"],
                "source_url": ["https://workbook.example"],
                "record_source": ["manual_workbook"],
                "match_origin": ["manual"],
                "is_manual_record": [True],
            }
        ),
        source_b_commercial_buildings=pd.DataFrame(),
        source_a_res_buildings=pd.DataFrame(),
        source_b_res_buildings=pd.DataFrame(),
        workbook_path=workbook_path,
        include_limit=0,
        source_c_frames=[],
    )

    workbook_plaza_rows = workbook_sheets["source_a_commercial"].loc[
        workbook_sheets["source_a_commercial"]["source_name_raw"] == "Workbook Plaza"
    ]
    assert len(workbook_plaza_rows) == 1
    assert workbook_plaza_rows.iloc[0]["row_kind"] == "manual_approved"


def test_build_consolidated_commercial_master_skips_blank_manual_names() -> None:
    consolidated = build_consolidated_commercial_master(
        source_a_buildings=pd.DataFrame(),
        source_b_buildings=pd.DataFrame(),
        manual_master=pd.DataFrame(
            {
                "canonical_building_name": [pd.NA],
                "normalized_name": ["BROKEN RECORD"],
                "district_name_en": ["Central"],
                "zone_en": ["HK Island"],
                "source_system": ["source_a_commercial"],
                "address": [pd.NA],
                "completion_year": [pd.NA],
                "management_company": [pd.NA],
            }
        ),
    )

    assert consolidated.empty
