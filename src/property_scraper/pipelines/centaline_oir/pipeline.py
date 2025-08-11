# src/kedro_centaline/pipelines/centaline_ici/pipeline.py

from kedro.pipeline import Pipeline, node
from kedro.pipeline.modular_pipeline import pipeline

from .nodes import (
    scrape_building_listings,
    scrape_building_details,
    scrape_transaction,
    join_transaction_with_building_details,
)

# src/kedro_centaline/pipelines/centaline_ici/pipeline.py

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(  # Existing node
                func=scrape_building_listings,
                inputs=["cl_oir_area_code", "params:webscraper"],
                outputs="centanet_oir_buildings_listing",
                name="scrape_building_listings",
            ),
            node(  # Existing node
                func=scrape_building_details,
                inputs=["centanet_oir_buildings_listing", "params:webscraper"],
                outputs="centanet_oir_buildings_details",
                name="scrape_building_details",
            ),
            node(  # Fixed scrape_transaction node
                func=scrape_transaction,
                inputs=["cl_oir_area_code", "params:webscraper"],
                outputs="centaline_oir_trans_lv_0",
                name="scrape_transaction",
            ),  # Added closing parenthesis
            node(
                func=join_transaction_with_building_details,
                inputs=[
                    "centaline_oir_trans_lv_0",
                    "centanet_oir_buildings_details",
                    "params:webscraper"
                ],
                outputs="centaline_oir_base",
                name="join_centaline_oir_data",
            )
           
        ]
    )
