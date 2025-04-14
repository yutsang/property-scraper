from kedro.pipeline import Pipeline, node
from kedro.pipeline.modular_pipeline import pipeline

from .nodes import (
    scrape_midland_buildings,
    process_buildings
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=scrape_midland_buildings,
                inputs=["ml_ici_area_code", "params:midland_ici"],
                outputs="midland_ici_building_listings",
                name="scrape_midland_buildings",
            ),
            
            node(
                func=process_buildings,
                inputs=["midland_ici_building_listings", "params:midland_ici"],
                outputs="midland_ici_building_details",
                name="scrape_midland_details"
            )
            
        ]
    )