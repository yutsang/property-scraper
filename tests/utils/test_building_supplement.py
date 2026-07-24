import pandas as pd

from property_scraper.utils.building_supplement import (
    apply_supplemental_matches,
    build_supplemental_review_queue,
    normalize_building_name,
)


def test_normalize_building_name_removes_punctuation_and_normalizes_case() -> None:
    assert normalize_building_name(" Queen's  Road West, 287 ") == "QUEENS ROAD WEST 287"


def test_build_supplemental_review_queue_groups_unmatched_rows_and_seeds_candidates() -> None:
    source_a_commercial = pd.DataFrame(
        {
            "propertyNameEn": ["One Plaza", "One Plaza", "No Match House", None, "None"],
            "_match_method": ["unmatched", "unmatched", "unmatched", "unmatched", "unmatched"],
            "districtNameEn": ["Central", "Central", "Wan Chai", "Central", "Central"],
            "zoneEn": ["HK Island", "HK Island", "HK Island", "HK Island", "HK Island"],
        }
    )
    source_b_commercial_base = pd.DataFrame(
        {
            "eng_name": ["One Plaza", "Other Building"],
            "chi_name": ["第一廣場", "其他大廈"],
            "building_id": ["B1", "B2"],
            "has_building_match": [False, False],
            "dist_name_en": ["Central", "Wan Chai"],
            "dist_code": ["CEN", "WAC"],
        }
    )
    source_b_commercial_primary = pd.DataFrame({"eng_name": ["ONE PLAZA", "STAR HOUSE"]})
    source_a_res = pd.DataFrame({"Name": ["Estate A"], "Tower": ["Tower 1"]})
    source_b_res = pd.DataFrame({"building": ["Block A"]})
    source_c_frames = [pd.DataFrame({"name": ["ONE PLAZA", "Leasing Hub Tower"]})]

    review_queue, audit_summary = build_supplemental_review_queue(
        source_a_commercial=source_a_commercial,
        source_b_commercial_base=source_b_commercial_base,
        source_b_commercial_primary=source_b_commercial_primary,
        source_a_res=source_a_res,
        source_b_res=source_b_res,
        source_c_frames=source_c_frames,
    )

    source_a_row = review_queue[
        (review_queue["source_system"] == "source_a_commercial")
        & (review_queue["source_name_raw"] == "One Plaza")
    ].iloc[0]
    assert source_a_row["transaction_count"] == 2
    assert source_a_row["candidate_building_name"] == "ONE PLAZA"
    assert source_a_row["candidate_source"] == "source_b_commercial_primary"
    assert source_a_row["review_status"] == "pending"
    assert "None" not in set(review_queue["source_name_raw"].astype(str))

    assert set(audit_summary["source_system"]) >= {
        "source_a_commercial",
        "source_b_commercial",
        "source_a_res",
        "source_b_res",
    }


def test_build_supplemental_review_queue_derives_zone_en_for_source_b_candidates() -> None:
    # source_b_commercial candidate rows previously always had zone_en hardcoded to
    # null, even though dist_code/dist_name_en were already available and Source A's
    # equivalent rows always populate zone_en from real data.
    source_b_commercial_base = pd.DataFrame(
        {
            "eng_name": ["Island Tower", "Kowloon Tower"],
            "building_id": ["B1", "B2"],
            "has_building_match": [False, False],
            "dist_name_en": ["Central", "Mongkok"],
            "dist_code": ["CEN", "MOK"],
        }
    )

    review_queue, _ = build_supplemental_review_queue(
        source_a_commercial=pd.DataFrame(),
        source_b_commercial_base=source_b_commercial_base,
        source_b_commercial_primary=pd.DataFrame(),
        source_a_res=pd.DataFrame(),
        source_b_res=pd.DataFrame(),
        source_c_frames=[],
    )

    island_row = review_queue.loc[review_queue["source_name_raw"] == "Island Tower"].iloc[0]
    kowloon_row = review_queue.loc[review_queue["source_name_raw"] == "Kowloon Tower"].iloc[0]
    assert island_row["zone_en"] == "HK Island"
    assert kowloon_row["zone_en"] == "Kowloon"


def test_apply_supplemental_matches_prefers_join_key_then_reviewed_name() -> None:
    unmatched = pd.DataFrame(
        {
            "id": ["tx-1", "tx-2", "tx-3"],
            "building_id": ["B100", "B200", None],
            "eng_name": ["Alpha Plaza", "Beta Centre", "Gamma House"],
            "dist_name_en": ["Central", "Wan Chai", "Central"],
        }
    )
    master = pd.DataFrame(
        {
            "domain": ["commercial", "commercial"],
            "source_system": ["source_b_commercial", "source_b_commercial"],
            "source_join_key": ["B100", None],
            "source_name_raw": ["Alpha Plaza", "Beta Centre"],
            "normalized_name": ["ALPHA PLAZA", "BETA CENTRE"],
            "district_name_en": ["Central", "Wan Chai"],
            "candidate_building_name": ["ALPHA PLAZA", "BETA CENTRE"],
            "canonical_building_name": ["Alpha Plaza", "Beta Centre"],
            "review_status": ["approved", "approved"],
            "completion_year": ["1999", "2005"],
        }
    )

    matched, remaining = apply_supplemental_matches(
        unmatched_df=unmatched,
        supplemental_master=master,
        source_system="source_b_commercial",
        domain="commercial",
        join_key_col="building_id",
        source_name_col="eng_name",
        district_name_col="dist_name_en",
        output_match_method_col="building_match_method",
    )

    assert len(matched) == 2
    assert len(remaining) == 1

    exact_row = matched.loc[matched["id"] == "tx-1"].iloc[0]
    assert exact_row["building_match_method"] == "supplement_exact_key"
    assert exact_row["matched_building_name"] == "Alpha Plaza"
    assert exact_row["completion_year"] == "1999"

    name_row = matched.loc[matched["id"] == "tx-2"].iloc[0]
    assert name_row["building_match_method"] == "supplement_reviewed_name"
    assert name_row["matched_building_name"] == "Beta Centre"


def test_apply_supplemental_matches_respects_zone_when_provided() -> None:
    unmatched = pd.DataFrame(
        {
            "id": ["tx-1"],
            "propertyNameEn": ["Sky Tower"],
            "districtNameEn": ["Central"],
            "zoneEn": ["Kowloon"],
        }
    )
    master = pd.DataFrame(
        {
            "domain": ["commercial"],
            "source_system": ["source_a_commercial"],
            "source_join_key": [None],
            "source_name_raw": ["Sky Tower"],
            "normalized_name": ["SKY TOWER"],
            "district_name_en": ["Central"],
            "zone_en": ["HK Island"],
            "candidate_building_name": ["SKY TOWER"],
            "canonical_building_name": ["Sky Tower"],
            "review_status": ["approved"],
        }
    )

    matched, remaining = apply_supplemental_matches(
        unmatched_df=unmatched,
        supplemental_master=master,
        source_system="source_a_commercial",
        domain="commercial",
        join_key_col=None,
        source_name_col="propertyNameEn",
        district_name_col="districtNameEn",
        zone_col="zoneEn",
        output_match_method_col="_match_method",
    )

    assert matched.empty
    assert len(remaining) == 1


def test_build_supplemental_review_queue_includes_source_b_rows_with_missing_match_flag() -> None:
    review_queue, _ = build_supplemental_review_queue(
        source_a_commercial=pd.DataFrame(),
        source_b_commercial_base=pd.DataFrame(
            {
                "eng_name": ["Mystery House"],
                "building_id": ["B1"],
                "has_building_match": [pd.NA],
                "dist_name_en": ["Central"],
                "dist_code": ["CEN"],
            }
        ),
        source_b_commercial_primary=pd.DataFrame(),
        source_a_res=pd.DataFrame(),
        source_b_res=pd.DataFrame(),
        source_c_frames=[],
    )

    assert "Mystery House" in set(review_queue["source_name_raw"])


def test_build_supplemental_review_queue_handles_source_b_frames_without_match_flag() -> None:
    review_queue, _ = build_supplemental_review_queue(
        source_a_commercial=pd.DataFrame(),
        source_b_commercial_base=pd.DataFrame(
            {
                "eng_name": ["Mystery House"],
                "building_id": ["B1"],
                "dist_name_en": ["Central"],
                "dist_code": ["CEN"],
            }
        ),
        source_b_commercial_primary=pd.DataFrame(),
        source_a_res=pd.DataFrame(),
        source_b_res=pd.DataFrame(),
        source_c_frames=[],
    )

    assert "Mystery House" in set(review_queue["source_name_raw"])
