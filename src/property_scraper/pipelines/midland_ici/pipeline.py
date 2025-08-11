from kedro.pipeline import Pipeline, node
from kedro.pipeline.modular_pipeline import pipeline

from .nodes import (
    scrape_midland_buildings,
    process_buildings,
    ml_ici_scrape_trans,
    midland_ici_join,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=scrape_midland_buildings,
                inputs=["ml_ici_area_code", "params:webscraper"],
                outputs="midland_ici_building_listings",
                name="scrape_midland_buildings",
            ),
            
            node(
                func=process_buildings,
                inputs=["midland_ici_building_listings", "params:webscraper"],
                outputs="midland_ici_building_details",
                name="scrape_midland_details"
            ),
            
            node(
                func=ml_ici_scrape_trans,
                inputs=["params:webscraper"],
                outputs="midland_ici_trans",
                name="midland_ici_transaction_scraper"
            ),
            node(
                func=midland_ici_join,
                inputs=["midland_ici_trans", "midland_ici_building_details", "params:webscraper"],
                outputs="midland_ici_base",
                name="join_midland_ici_data"
            )

            
        ]
    )
    
# Midland ICI: midland_ici_trans join midland_ici_building_details