# src/kedro_centaline/pipelines/centaline_ici/pipeline.py

from kedro.pipeline import Pipeline, node
from kedro.pipeline.modular_pipeline import pipeline

from .nodes import (
    scrape_building_listings,
    scrape_building_details,
    scrape_transaction,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=scrape_building_listings,
                inputs=["cl_oir_area_code", "params:centaline_oir"],
                outputs="centanet_oir_buildings_listing",
                name="scrape_building_listings",
            ),
            
            node(
                func=scrape_building_details,
                inputs=["centanet_oir_buildings_listing", "params:centaline_oir"],
                outputs="centanet_oir_buildings_details",
                name="scrape_building_details",
            ),
            
            node(
                func=scrape_transaction,
                inputs=["cl_oir_area_code", "params:centaline_oir"],
                outputs="centaline_oir_trans_lv_0",
                name="scrape_transaction",
            )
           
        ]
    )
