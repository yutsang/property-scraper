import pandas as pd

from property_scraper.utils.building_supplement import (
    apply_supplemental_matches,
    build_supplemental_review_queue,
    normalize_building_name,
)


def test_normalize_building_name_removes_punctuation_and_normalizes_case() -> None:
    assert normalize_building_name(" Queen's  Road West, 287 ") == "QUEENS ROAD WEST 287"


def test_build_supplemental_review_queue_groups_unmatched_rows_and_seeds_candidates() -> None:
    centaline_oir = pd.DataFrame(
        {
            "propertyNameEn": ["One Plaza", "One Plaza", "No Match House", None, "None"],
            "_match_method": ["unmatched", "unmatched", "unmatched", "unmatched", "unmatched"],
            "districtNameEn": ["Central", "Central", "Wan Chai", "Central", "Central"],
            "zoneEn": ["HK Island", "HK Island", "HK Island", "HK Island", "HK Island"],
        }
    )
    midland_ici_base = pd.DataFrame(
        {
            "eng_name": ["One Plaza", "Other Building"],
            "chi_name": ["第一廣場", "其他大廈"],
            "building_id": ["B1", "B2"],
            "has_building_match": [False, False],
            "dist_name_en": ["Central", "Wan Chai"],
            "dist_code": ["CEN", "WAC"],
        }
    )
    midland_ici_primary = pd.DataFrame({"eng_name": ["ONE PLAZA", "STAR HOUSE"]})
    centaline_res = pd.DataFrame({"Name": ["Estate A"], "Tower": ["Tower 1"]})
    midland_res = pd.DataFrame({"building": ["Block A"]})
    leasinghub_frames = [pd.DataFrame({"name": ["ONE PLAZA", "Leasing Hub Tower"]})]

    review_queue, audit_summary = build_supplemental_review_queue(
        centaline_oir=centaline_oir,
        midland_ici_base=midland_ici_base,
        midland_ici_primary=midland_ici_primary,
        centaline_res=centaline_res,
        midland_res=midland_res,
        leasinghub_frames=leasinghub_frames,
    )

    centaline_row = review_queue[
        (review_queue["source_system"] == "centaline_oir")
        & (review_queue["source_name_raw"] == "One Plaza")
    ].iloc[0]
    assert centaline_row["transaction_count"] == 2
    assert centaline_row["candidate_building_name"] == "ONE PLAZA"
    assert centaline_row["candidate_source"] == "midland_ici_primary"
    assert centaline_row["review_status"] == "pending"
    assert "None" not in set(review_queue["source_name_raw"].astype(str))

    assert set(audit_summary["source_system"]) >= {
        "centaline_oir",
        "midland_ici",
        "centaline_res",
        "midland_res",
    }


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
            "source_system": ["midland_ici", "midland_ici"],
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
        source_system="midland_ici",
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
            "source_system": ["centaline_oir"],
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
        source_system="centaline_oir",
        domain="commercial",
        join_key_col=None,
        source_name_col="propertyNameEn",
        district_name_col="districtNameEn",
        zone_col="zoneEn",
        output_match_method_col="_match_method",
    )

    assert matched.empty
    assert len(remaining) == 1


def test_build_supplemental_review_queue_includes_midland_rows_with_missing_match_flag() -> None:
    review_queue, _ = build_supplemental_review_queue(
        centaline_oir=pd.DataFrame(),
        midland_ici_base=pd.DataFrame(
            {
                "eng_name": ["Mystery House"],
                "building_id": ["B1"],
                "has_building_match": [pd.NA],
                "dist_name_en": ["Central"],
                "dist_code": ["CEN"],
            }
        ),
        midland_ici_primary=pd.DataFrame(),
        centaline_res=pd.DataFrame(),
        midland_res=pd.DataFrame(),
        leasinghub_frames=[],
    )

    assert "Mystery House" in set(review_queue["source_name_raw"])


def test_build_supplemental_review_queue_handles_midland_frames_without_match_flag() -> None:
    review_queue, _ = build_supplemental_review_queue(
        centaline_oir=pd.DataFrame(),
        midland_ici_base=pd.DataFrame(
            {
                "eng_name": ["Mystery House"],
                "building_id": ["B1"],
                "dist_name_en": ["Central"],
                "dist_code": ["CEN"],
            }
        ),
        midland_ici_primary=pd.DataFrame(),
        centaline_res=pd.DataFrame(),
        midland_res=pd.DataFrame(),
        leasinghub_frames=[],
    )

    assert "Mystery House" in set(review_queue["source_name_raw"])
