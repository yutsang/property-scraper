from kedro.pipeline import Pipeline, node
from .nodes import (
    load_district_data,
    process_estate_data,
    normalize_all_columns
)

def create_pipeline(**kwargs) -> Pipeline:
    """
    Create the midland estates pipeline
    
    Returns:
        Pipeline: A pipeline to fetch and process estates data
    """
    return Pipeline(
        [
            node(
                func=load_district_data,
                inputs="midland_res_area_code",
                outputs="cleaned_district_data",
                name="load_and_clean_district_data",
            ),
            node(
                func=process_estate_data,
                inputs="cleaned_district_data",
                outputs="raw_estate_data",
                name="fetch_and_process_estate_data",
            ),
            node(
                func=normalize_all_columns,
                inputs="raw_estate_data",
                outputs="processed_estate_data",
                name="normalize_estate_data",
            ),
        ]
    )
