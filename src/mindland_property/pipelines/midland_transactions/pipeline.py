# src/midland_property/pipelines/midland_transactions/pipeline.py
from kedro.pipeline import Pipeline, node, pipeline
from .nodes import fetch_transactions

def create_pipeline(**kwargs) -> Pipeline:
    """
    Create the midland transactions pipeline
    
    Returns:
        Pipeline: A pipeline to fetch transaction data
    """
    return pipeline(
        [
            node(
                func=fetch_transactions,
                inputs="midland_res_area_code",
                outputs="midland_res_transactions",
                name="fetch_midland_transactions",
            ),
        ]
    )
