from kedro.pipeline import Pipeline, node
from kedro.pipeline.modular_pipeline import pipeline

from .nodes import (
    load_existing_source_a_commercial_snapshots,
    scrape_building_listings,
    scrape_building_details,
    scrape_transaction,
    join_transaction_with_building_details,
)
from ...utils.building_supplement import (
    load_source_a_commercial_supplemental_master,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=load_existing_source_a_commercial_snapshots,
                inputs="params:webscraper",
                outputs=[
                    "existing_source_a_commercial_buildings",
                    "existing_source_a_commercial_details",
                    "existing_source_a_commercial_transactions",
                ],
                name="load_existing_source_a_commercial_snapshots",
            ),
            node(
                func=scrape_building_listings,
                inputs=[
                    "source_a_commercial_area_code",
                    "existing_source_a_commercial_buildings",
                    "params:webscraper",
                ],
                outputs="source_a_commercial_buildings_listing",
                name="scrape_building_listings",
            ),
            node(
                func=scrape_building_details,
                inputs=[
                    "source_a_commercial_buildings_listing",
                    "existing_source_a_commercial_details",
                    "params:webscraper",
                ],
                outputs="source_a_commercial_buildings_details",
                name="scrape_building_details",
            ),
            node(
                func=scrape_transaction,
                inputs=[
                    "source_a_commercial_area_code",
                    "existing_source_a_commercial_transactions",
                    "params:webscraper",
                ],
                outputs="source_a_commercial_trans_lv_0",
                name="scrape_transaction",
            ),
            node(
                func=load_source_a_commercial_supplemental_master,
                inputs="params:webscraper",
                outputs="source_a_commercial_supplemental_master",
                name="load_source_a_commercial_supplemental_master",
            ),
            node(
                func=join_transaction_with_building_details,
                inputs=[
                    "source_a_commercial_trans_lv_0",
                    "source_a_commercial_buildings_details",
                    "source_a_commercial_supplemental_master",
                    "params:webscraper"
                ],
                outputs="source_a_commercial_base",
                name="join_source_a_commercial_data",
            )
           
        ]
    )
