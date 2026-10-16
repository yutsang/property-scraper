import pandas as pd
import pytest

from property_scraper.pipeline_registry import register_pipelines
from property_scraper.pipelines.data_process.nodes import (
    select_source_a_commercial_columns,
    select_source_a_res_columns,
    select_source_b_commercial_columns,
    select_source_b_res_columns,
)
from property_scraper.pipelines.data_process.quality import (
    validate_publication_inputs,
)
from property_scraper.pipelines.data_process.workbook import assemble_workbooks

PRIMARY_SCHEMAS = {
    "source_a_res": [
        "date", "region", "district", "subdistrict", "Name", "title_lg",
        "Tower", "Floor", "Flat", "transaction_type", "area", "price",
        "ft_price", "g_area", "g_unit_price", "area_provenance",
        "completion_year", "age", "source", "address", "rooms",
        "property_type", "building_code",
    ],
    "source_b_res": [
        "date", "region_name_trans", "subregion", "district", "estate",
        "building", "news_name", "floor", "floor_level", "flat", "tx_type",
        "price", "net_area", "area_provenance", "unit_price_net",
        "building_first_op_date", "completion_year", "age", "developer_name",
        "source", "address", "url_desc_trans", "location", "last_tx_date",
        "last_price", "mkt_type", "area", "total_unit_count",
        "total_block_count", "primary_school_net",
        "market_stat_monthly_0_date",
    ],
    "source_a_commercial": [
        "date", "zoneEn", "districtNameEn", "propertyUsageDisplayName",
        "grade", "propertyNameEn", "propertyNameCn", "floor", "unit",
        "transactionType", "transactionArea", "net_area", "area_provenance",
        "price", "avgPrice", "full_address", "completion_year", "age",
        "source_url", "management_company", "developers", "carpark",
        "sourceDisplayName", "Datasource", "id", "matched_building_name",
        "match_score", "_match_method", "_match_score",
    ],
    "source_b_commercial": [
        "date", "zoneEn", "dist_code", "dist_name_zh", "dist_name_en",
        "ics_type", "Grade", "eng_name", "chi_name", "floor", "flat",
        "tx_type", "area", "net_area", "area_provenance", "price",
        "price_per_feet", "street_id", "street_name_zh", "street_name_en",
        "streetno", "Completion", "age", "area1", "area_desc1", "area2",
        "area_desc2", "area3", "area_desc3", "area4", "area_desc4", "URL",
        "upload_source", "No. of Floors",
        "Management Fee (Approx. per sq. ft.)", "Floor Remark",
        "Management Company", "has_building_match", "building_match_method",
        "matched_building_name", "supplement_candidate_source",
        "supplement_review_status", "match_origin", "record_source",
        "Datasource",
    ],
}


SELECTOR_CASES = [
    (
        "source_a_res",
        select_source_a_res_columns,
        PRIMARY_SCHEMAS["source_a_res"],
    ),
    (
        "source_b_res",
        select_source_b_res_columns,
        ["tx_date", *PRIMARY_SCHEMAS["source_b_res"][1:]],
    ),
    (
        "source_a_commercial",
        select_source_a_commercial_columns,
        ["transactionDate", *PRIMARY_SCHEMAS["source_a_commercial"][1:]],
    ),
    (
        "source_b_commercial",
        select_source_b_commercial_columns,
        PRIMARY_SCHEMAS["source_b_commercial"],
    ),
]


@pytest.mark.parametrize(("source", "selector", "input_columns"), SELECTOR_CASES)
def test_primary_output_ordered_schema_contract(
    source,
    selector,
    input_columns,
) -> None:
    result = selector(
        pd.DataFrame(columns=input_columns),
        {"area_conversion": {"enabled": False}},
    )

    assert list(result.columns) == PRIMARY_SCHEMAS[source]


def test_registered_pipeline_topology_contract() -> None:
    pipelines = register_pipelines()

    assert set(pipelines) == {
        "source_b_res",
        "source_b_commercial",
        "source_a_res",
        "source_a_commercial",
        "source_a",
        "source_b",
        "residential",
        "commercial",
        "data_process",
        "building_master",
        "__all__",
        "__default__",
    }
    assert {node.name for node in pipelines["data_process"].nodes} == {
        "cleanse_source_a_res",
        "select_source_a_res_columns",
        "cleanse_source_a_commercial",
        "select_source_a_commercial_columns",
        "cleanse_source_b_res",
        "select_source_b_res_columns",
        "build_residential_quality_report",
        "build_run_statistics",
        "cleanse_source_b_commercial",
        "select_source_b_commercial_columns",
        "validate_primary_for_publication",
        "merge_and_to_excel",
    }
    assert {node.name for node in pipelines["building_master"].nodes} == {
        "pull_centaline_building_blocks",
        "pull_midland_estate_buildings",
        "adapt_centaline_residential_buildings",
        "adapt_midland_residential_buildings",
        "adapt_centaline_commercial_buildings",
        "adapt_midland_commercial_buildings",
        "combine_building_source_records",
        "generate_building_match_candidates",
        "generate_fingerprint_candidates",
        "generate_commercial_fingerprint_candidates",
        "combine_candidate_pairs",
        "resolve_canonical_buildings",
        "triage_building_review_queue",
        "build_building_master_outputs",
        "build_commercial_building_compatibility",
    }


def test_workbook_date_windows_and_row_multisets() -> None:
    dates = [
        "2019-12-31",
        "2020-01-01",
        "2023-12-31",
        "2024-01-01",
        "2027-01-01",
    ]
    frames = {
        source: pd.DataFrame({"date": dates, "record_id": range(5)})
        for source in PRIMARY_SCHEMAS
    }
    labels = {
        "source_a_res": "Centaline Residential",
        "source_b_res": "Midland Residential",
        "source_a_commercial": "Centaline OIR",
        "source_b_commercial": "Midland ICI",
        "overlap_res": "Overlap_Res",
        "overlap_com": "Overlap_Com",
    }

    outputs, metrics = assemble_workbooks(frames, labels, end_year=2026)

    expected_ids = {
        "historical": {1, 2},
        "recent": {3},
    }
    assert set(
        outputs["residential_2020_2023"]["Centaline Residential"]["record_id"]
    ) == expected_ids["historical"]
    assert set(
        outputs["residential_2024_current"]["Midland Residential"]["record_id"]
    ) == expected_ids["recent"]
    assert set(
        outputs["commercial_2020_2023"]["Centaline OIR"]["record_id"]
    ) == expected_ids["historical"]
    assert set(
        outputs["commercial_2024_current"]["Midland ICI"]["record_id"]
    ) == expected_ids["recent"]
    assert metrics["source_a_res"] == {
        "total": 5,
        "invalid_dates": 0,
        "before_start": 1,
        "after_end": 1,
        "historical": 2,
        "recent": 1,
        "min_year": 2019,
        "max_year": 2027,
    }


def test_publication_gate_rejects_invalid_dates() -> None:
    with pytest.raises(ValueError, match="source_a_res: 1 invalid dates"):
        validate_publication_inputs(
            {
                "source_a_res": pd.DataFrame(
                    {"date": ["not-a-date"], "completion_year": [2000]}
                )
            }
        )
