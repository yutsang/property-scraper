# src/midland_property/pipelines/midland_data_processing/pipeline.py
from kedro.pipeline import Pipeline, node, pipeline
from .nodes import merge_transactions_estates, calculate_price_metrics

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=merge_transactions_estates,
            inputs=["midland_res_transactions", "midland_res_estates"],
            outputs="merged_property_data",
            name="merge_transactions_estates"
        ),
        node(
            func=calculate_price_metrics,
            inputs="merged_property_data",
            outputs="processed_property_data",
            name="calculate_price_metrics"
        )
    ])
