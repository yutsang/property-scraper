from kedro.pipeline import Pipeline, node, pipeline
from .health import (
    check_centaline_api_health_node,
    update_area_codes_from_sitemap,
)
from .estates import (
    scrape_estate_listings,
    scrape_estate_details,
)
from .nodes import (
    scrape_transaction_data,
    process_transaction_data,
    enrich_estate_data,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=check_centaline_api_health_node,
            inputs=["params:webscraper"],
            name="check_api_health",
        ),
        node(
            func=update_area_codes_from_sitemap,
            inputs=["params:webscraper"],
            outputs="area_codes",
            name="update_area_codes",
        ),
        node(
            func=scrape_estate_listings,
            inputs=["area_codes", "params:webscraper"],
            outputs="raw_estate_listings",
            name="estate_listing_scraper"
        ),

        node(
            func=scrape_estate_details,
            inputs=["raw_estate_listings", "params:webscraper"],
            outputs="estate_details_raw",
            name="estate_detail_scraper"
        ),
        node(
            func=scrape_transaction_data,
            inputs=["area_codes", "params:webscraper"],
            outputs="raw_transaction_data",
            name="transaction_data_scraper"
        ),
        node(
            func=process_transaction_data,
            inputs=["raw_transaction_data", "params:webscraper"],
            outputs="processed_transactions",
            name="transaction_processor"
        ),
        node(
            func=enrich_estate_data,
            inputs=["estate_details_raw", "processed_transactions", "params:webscraper"],
            outputs="centaline_res_base",
            name="estate_data_enricher"
        )
    ])
