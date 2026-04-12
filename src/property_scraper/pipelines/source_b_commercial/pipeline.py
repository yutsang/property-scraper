from kedro.pipeline import Pipeline, node
from kedro.pipeline.modular_pipeline import pipeline

from .nodes import (
    scrape_source_b_buildings,
    process_buildings,
    source_b_commercial_scrape_trans,
    source_b_commercial_join,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=scrape_source_b_buildings,
                inputs=["source_b_commercial_area_code", "params:webscraper"],
                outputs="source_b_commercial_building_listings",
                name="scrape_source_b_buildings",
            ),
            
            node(
                func=process_buildings,
                inputs=["source_b_commercial_building_listings", "params:webscraper"],
                outputs="source_b_commercial_building_details",
                name="scrape_source_b_commercial_details"
            ),
            
            node(
                func=source_b_commercial_scrape_trans,
                inputs=["params:webscraper"],
                outputs="source_b_commercial_trans",
                name="source_b_commercial_transaction_scraper"
            ),
            node(
                func=source_b_commercial_join,
                inputs=["source_b_commercial_trans", "source_b_commercial_building_details", "params:webscraper"],
                outputs="source_b_commercial_base",
                name="join_source_b_commercial_data"
            )

            
        ]
    )
    
# Source B commercial: source_b_commercial_trans join source_b_commercial_building_details