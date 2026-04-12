from kedro.pipeline import Pipeline, node, pipeline
from .nodes import fetch_district_codes
from .nodes import merge_transactions_estates #, calculate_price_metrics
from .nodes import load_district_data, process_estate_data, normalize_all_columns
from .nodes import fetch_transactions

def create_pipeline(**kwargs) -> Pipeline:
    """
    Create the Source B residential pipeline.
    
    Returns:
        Pipeline: A pipeline to fetch district codes
    """

    return Pipeline(
        [
            # Step 1: Fetch district codes
            node(
                func=fetch_district_codes,
                inputs=["params:webscraper"],
                outputs="source_b_res_area_code",
                name="fetch_source_b_district_codes",
            ),
            
            # Step 2a: Clean district data
            node(
                func=load_district_data,
                inputs=["source_b_res_area_code", "params:webscraper"],
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
                outputs="source_b_res_estates",
                name="normalize_estate_data",
            ),
            
            # Step 2d: Fetch transaction data - FIXED INPUT
            node(
                func=fetch_transactions,
                inputs=["cleaned_district_data", "params:webscraper"],
                outputs="source_b_res_transactions",
                name="fetch_source_b_transactions",
            ),
            
            # Step 3: Merge transaction and estate data
            node(
                func=merge_transactions_estates,
                inputs=["source_b_res_transactions", "source_b_res_estates"],
                outputs="source_b_res_base",
                name="merge_transactions_estates"
            )            

        ]
    )

