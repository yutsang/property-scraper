"""Kedro wiring for the cross-source building master."""

from kedro.pipeline import Pipeline, node, pipeline

from .fingerprints import (
    combine_candidate_pairs,
    generate_commercial_fingerprint_candidates,
    generate_residential_fingerprint_candidates,
)
from .matching import (
    build_building_master_outputs,
    build_commercial_compatibility_master,
    generate_match_candidates,
    resolve_canonical_buildings,
)
from .buildings import pull_centaline_building_blocks, pull_midland_estate_buildings
from .nodes import (
    adapt_centaline_commercial,
    adapt_centaline_residential,
    adapt_midland_commercial,
    adapt_midland_residential,
    combine_source_records,
)
from .review import triage_building_review_queue


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                pull_centaline_building_blocks,
                ["raw_estate_listings", "parameters"],
                "centaline_building_blocks",
                name="pull_centaline_building_blocks",
            ),
            node(
                pull_midland_estate_buildings,
                ["source_b_res_estates", "parameters"],
                "midland_estate_buildings",
                name="pull_midland_estate_buildings",
            ),
            node(
                adapt_centaline_residential,
                ["estate_details_raw", "source_a_res_base"],
                "centaline_residential_building_records",
                name="adapt_centaline_residential_buildings",
            ),
            node(
                adapt_midland_residential,
                ["source_b_res_estates", "source_b_res_base"],
                "midland_residential_building_records",
                name="adapt_midland_residential_buildings",
            ),
            node(
                adapt_centaline_commercial,
                [
                    "source_a_commercial_buildings_details",
                    "source_a_commercial_base",
                ],
                "centaline_commercial_building_records",
                name="adapt_centaline_commercial_buildings",
            ),
            node(
                adapt_midland_commercial,
                [
                    "source_b_commercial_building_details",
                    "source_b_commercial_base",
                ],
                "midland_commercial_building_records",
                name="adapt_midland_commercial_buildings",
            ),
            node(
                combine_source_records,
                [
                    "centaline_residential_building_records",
                    "midland_residential_building_records",
                    "centaline_commercial_building_records",
                    "midland_commercial_building_records",
                ],
                "building_master_source_records",
                name="combine_building_source_records",
            ),
            node(
                generate_match_candidates,
                [
                    "building_master_source_records",
                    "params:building_master_matching",
                ],
                "building_master_name_candidates",
                name="generate_building_match_candidates",
            ),
            node(
                generate_residential_fingerprint_candidates,
                [
                    "building_master_source_records",
                    "source_a_res_base",
                    "source_b_res_base",
                    "params:building_master_matching",
                ],
                "building_master_fingerprint_candidates",
                name="generate_fingerprint_candidates",
            ),
            node(
                generate_commercial_fingerprint_candidates,
                [
                    "building_master_source_records",
                    "source_a_commercial_base",
                    "source_b_commercial_base",
                    "params:building_master_matching",
                ],
                "building_master_commercial_fingerprint_candidates",
                name="generate_commercial_fingerprint_candidates",
            ),
            node(
                combine_candidate_pairs,
                [
                    "building_master_name_candidates",
                    "building_master_fingerprint_candidates",
                    "building_master_commercial_fingerprint_candidates",
                ],
                "building_master_candidate_pairs",
                name="combine_candidate_pairs",
            ),
            node(
                resolve_canonical_buildings,
                [
                    "building_master_source_records",
                    "building_master_candidate_pairs",
                    "building_master_reviewed_overrides",
                    "params:building_master_matching",
                ],
                [
                    "canonical_building_master",
                    "building_source_bridge",
                    "building_master_raw_review_queue",
                ],
                name="resolve_canonical_buildings",
            ),
            node(
                triage_building_review_queue,
                [
                    "building_master_raw_review_queue",
                    "building_master_source_records",
                    "params:building_master_review",
                ],
                [
                    "building_master_review_queue",
                    "building_master_override_template",
                    "building_master_review_summary",
                ],
                name="triage_building_review_queue",
            ),
            node(
                build_building_master_outputs,
                [
                    "canonical_building_master",
                    "building_source_bridge",
                    "building_master_review_queue",
                    "building_master_candidate_pairs",
                ],
                [
                    "consolidated_building_workbook",
                    "building_master_match_audit",
                ],
                name="build_building_master_outputs",
            ),
            node(
                build_commercial_compatibility_master,
                "canonical_building_master",
                "building_master_commercial_compatibility",
                name="build_commercial_building_compatibility",
            ),
        ]
    )
