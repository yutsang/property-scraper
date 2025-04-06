from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    scrape_estate_listings,
    scrape_estate_details,
    scrape_transaction_data,
    process_transaction_data,
    enrich_estate_data,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=scrape_estate_listings,
            inputs=["area_codes", "params:scraper"],
            outputs="raw_estate_listings",
            name="estate_listing_scraper"
        ),

        node(
            func=scrape_estate_details,
            inputs=["raw_estate_listings", "params:scraper"],
            outputs="estate_details_raw",
            name="estate_detail_scraper"
        ),
        node(
            func=scrape_transaction_data,
            inputs=["area_codes", "params:scraper"],
            outputs="raw_transaction_data",
            name="transaction_data_scraper"
        ),
        node(
            func=process_transaction_data,
            inputs=["raw_transaction_data", "params:processing"],
            outputs="processed_transactions",
            name="transaction_processor"
        ),
        node(
            func=enrich_estate_data,
            inputs=["estate_details_raw", "processed_transactions"],
            outputs="enriched_estate_data",
            name="estate_data_enricher"
        )
    ])
