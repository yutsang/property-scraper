from kedro.pipeline import Pipeline, node, pipeline
from .nodes import fetch_district_codes
from .nodes import merge_transactions_estates #, calculate_price_metrics
from .nodes import load_district_data, process_estate_data, normalize_all_columns
from .nodes import fetch_transactions

def create_pipeline(**kwargs) -> Pipeline:
    """
    Create the midland area codes pipeline
    
    Returns:
        Pipeline: A pipeline to fetch district codes
    """

    return Pipeline(
        [
            # Step 1: Fetch district codes
            node(
                func=fetch_district_codes,
                inputs=None,
                outputs="midland_res_area_code",
                name="fetch_midland_district_codes",
            ),
            
            # Step 2a: Clean district data
            node(
                func=load_district_data,
                inputs=["midland_res_area_code", "params:webscraper"],
                outputs="cleaned_district_data",
                name="load_and_clean_district_data",
            ),
            
            # Step 2b: Fetch and process estate data
            node(
                func=process_estate_data,
                inputs=["cleaned_district_data", "params:webscraper"],
                outputs="raw_estate_data",
                name="fetch_and_process_estate_data",
            ),
            
            # Step 2c: Normalize estate data - FIXED OUTPUT NAME
            node(
                func=normalize_all_columns,
                inputs="raw_estate_data",
                outputs="midland_res_estates",  # Changed from "processed_estate_data"
                name="normalize_estate_data",
            ),
            
            # Step 2d: Fetch transaction data - FIXED INPUT
            node(
                func=fetch_transactions,
                inputs=["cleaned_district_data", "params:webscraper"],  # Changed from "midland_res_area_code"
                outputs="midland_res_transactions",
                name="fetch_midland_transactions",
            ),
            
            # Step 3: Merge transaction and estate data
            node(
                func=merge_transactions_estates,
                inputs=["midland_res_transactions", "midland_res_estates"],
                outputs="midland_res_base",
                name="merge_transactions_estates"
            )            

        ]
    )

