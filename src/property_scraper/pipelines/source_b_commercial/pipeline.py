from kedro.pipeline import Pipeline, node
from kedro.pipeline.modular_pipeline import pipeline

from .nodes import (
    load_existing_source_b_commercial_snapshots,
    scrape_source_b_buildings,
    process_buildings,
    source_b_commercial_scrape_trans,
    source_b_commercial_join,
)
from ...utils.building_supplement import (
    load_source_b_commercial_supplemental_master,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=load_existing_source_b_commercial_snapshots,
                inputs="params:webscraper",
                outputs=[
                    "existing_source_b_commercial_buildings",
                    "existing_source_b_commercial_details",
                    "existing_source_b_commercial_transactions",
                ],
                name="load_existing_source_b_commercial_snapshots",
            ),
            node(
                func=scrape_source_b_buildings,
                inputs=[
                    "source_b_commercial_area_code",
                    "existing_source_b_commercial_buildings",
                    "params:webscraper",
                ],
                outputs="source_b_commercial_building_listings",
                name="scrape_source_b_buildings",
            ),
            
            node(
                func=process_buildings,
                inputs=[
                    "source_b_commercial_building_listings",
                    "existing_source_b_commercial_details",
                    "params:webscraper",
                ],
                outputs="source_b_commercial_building_details",
                name="scrape_source_b_commercial_details"
            ),
            
            node(
                func=source_b_commercial_scrape_trans,
                inputs=[
                    "existing_source_b_commercial_transactions",
                    "params:webscraper",
                ],
                outputs="source_b_commercial_trans",
                name="source_b_commercial_transaction_scraper"
            ),
            node(
                func=load_source_b_commercial_supplemental_master,
                inputs="params:webscraper",
                outputs="source_b_commercial_supplemental_master",
                name="load_source_b_commercial_supplemental_master",
            ),
            node(
                func=source_b_commercial_join,
                inputs=[
                    "source_b_commercial_trans",
                    "source_b_commercial_building_details",
                    "source_b_commercial_supplemental_master",
                    "params:webscraper",
                ],
                outputs="source_b_commercial_base",
                name="join_source_b_commercial_data"
            )

            
        ]
    )
    
# Source B commercial: source_b_commercial_trans join source_b_commercial_building_details