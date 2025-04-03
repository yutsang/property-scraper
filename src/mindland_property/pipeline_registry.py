# src/midland_property/pipeline_registry.py
from kedro.pipeline import Pipeline
from midland_property.pipelines import (
    midland_area_codes,
    midland_transactions,
    midland_estates,
    midland_data_processing
)

def register_pipelines() -> dict:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    # Individual pipelines
    area_codes_pipeline = midland_area_codes.create_pipeline()
    transactions_pipeline = midland_transactions.create_pipeline()
    estates_pipeline = midland_estates.create_pipeline()
    data_processing_pipeline = midland_data_processing.create_pipeline()
    
    # Combined pipeline
    midland_res_pipeline = (
        area_codes_pipeline + 
        transactions_pipeline + 
        estates_pipeline + 
        data_processing_pipeline
    )
    
    return {
        "midland_area_codes": area_codes_pipeline,
        "midland_transactions": transactions_pipeline,
        "midland_estates": estates_pipeline,
        "midland_data_processing": data_processing_pipeline,
        "midland_res": midland_res_pipeline,
        "__default__": midland_res_pipeline,
    }
