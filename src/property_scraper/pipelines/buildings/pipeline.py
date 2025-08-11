"""
Centralized Buildings Pipeline
Handles building information consolidation for both commercial and residential properties.
Implements multi-stage matching: exact matching → OSM geocoding → Google Maps API (initialization only)
"""

from kedro.pipeline import Pipeline, node
from kedro.pipeline.modular_pipeline import pipeline

from .nodes import (
    extract_building_data_from_sources,
    create_consolidated_building_databases,
    initialize_with_google_maps,
    create_manual_review_list,
    apply_manual_corrections,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            # Step 1: Extract building data from all sources
            node(
                func=extract_building_data_from_sources,
                inputs=[
                    "centaline_res_base",
                    "centaline_oir_base", 
                    "midland_res_base",
                    "midland_ici_base",
                    "leasinghub_building_listings",
                    "params:buildings"
                ],
                outputs=["commercial_buildings_raw", "residential_buildings_raw"],
                name="extract_building_data_from_sources",
            ),
            
            # Step 2: Create consolidated building databases with multi-stage matching
            node(
                func=create_consolidated_building_databases,
                inputs=[
                    "commercial_buildings_raw",
                    "residential_buildings_raw",
                    "params:buildings"
                ],
                outputs=[
                    "consolidated_commercial_db", 
                    "consolidated_residential_db",
                    "unmatched_commercial_buildings",
                    "unmatched_residential_buildings"
                ],
                name="create_consolidated_building_databases",
            ),
            
            # Step 3: Initialize unmatched buildings with Google Maps API (optional, expensive)
            node(
                func=initialize_with_google_maps,
                inputs=[
                    "unmatched_commercial_buildings",
                    "params:buildings"
                ],
                outputs="google_geocoded_commercial_buildings",
                name="initialize_commercial_with_google_maps",
            ),
            
            node(
                func=initialize_with_google_maps,
                inputs=[
                    "unmatched_residential_buildings",
                    "params:buildings"
                ],
                outputs="google_geocoded_residential_buildings",
                name="initialize_residential_with_google_maps",
            ),
            
            # Step 4: Create manual review list for unmatched buildings
            node(
                func=create_manual_review_list,
                inputs=[
                    "google_geocoded_commercial_buildings",
                    "google_geocoded_residential_buildings",
                    "params:buildings"
                ],
                outputs="manual_review_list",
                name="create_manual_review_list",
            ),
            
            # Step 5: Apply manual corrections (optional, requires manual input)
            node(
                func=apply_manual_corrections,
                inputs=[
                    "consolidated_commercial_db",
                    "consolidated_residential_db",
                    "manual_review_list",
                    "params:buildings"
                ],
                outputs=["final_consolidated_commercial_db", "final_consolidated_residential_db"],
                name="apply_manual_corrections",
            ),
        ]
    ) 