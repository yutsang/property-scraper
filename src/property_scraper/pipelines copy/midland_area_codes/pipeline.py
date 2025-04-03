from kedro.pipeline import Pipeline, node, pipeline
from .nodes import fetch_district_codes

def create_pipeline(**kwargs) -> Pipeline:
    """
    Create the midland area codes pipeline
    
    Returns:
        Pipeline: A pipeline to fetch district codes
    """
    return Pipeline(
        [
            node(
                func=fetch_district_codes,
                inputs=None,
                outputs="midland_res_area_code",
                name="fetch_midland_district_codes",
            ),
        ]
    )
