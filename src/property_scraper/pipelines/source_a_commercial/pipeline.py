from kedro.pipeline import Pipeline, node
from kedro.pipeline.modular_pipeline import pipeline

from .nodes import (
    scrape_building_listings,
    scrape_building_details,
    scrape_transaction,
    join_transaction_with_building_details,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=scrape_building_listings,
                inputs=["source_a_commercial_area_code", "params:webscraper"],
                outputs="source_a_commercial_buildings_listing",
                name="scrape_building_listings",
            ),
            node(
                func=scrape_building_details,
                inputs=["source_a_commercial_buildings_listing", "params:webscraper"],
                outputs="source_a_commercial_buildings_details",
                name="scrape_building_details",
            ),
            node(
                func=scrape_transaction,
                inputs=["source_a_commercial_area_code", "params:webscraper"],
                outputs="source_a_commercial_trans_lv_0",
                name="scrape_transaction",
            ),
            node(
                func=join_transaction_with_building_details,
                inputs=[
                    "source_a_commercial_trans_lv_0",
                    "source_a_commercial_buildings_details",
                    "params:webscraper"
                ],
                outputs="source_a_commercial_base",
                name="join_source_a_commercial_data",
            )
           
        ]
    )
