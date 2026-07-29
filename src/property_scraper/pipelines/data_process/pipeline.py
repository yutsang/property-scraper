from kedro.pipeline import Pipeline, node, pipeline
from .nodes import cleanse_source_a_res, cleanse_source_a_commercial
from .nodes import cleanse_source_b_commercial, cleanse_source_b_res
from .nodes import build_residential_quality_report, merge_and_excel
from .nodes import select_source_a_res_columns, select_source_a_commercial_columns
from .nodes import select_source_b_res_columns, select_source_b_commercial_columns

def create_pipeline(**kwargs) -> Pipeline:
    """
    Create the combined processing pipeline.
    
    Returns:
        Pipeline: A pipeline to fetch district codes
    """

    return Pipeline(
        [
            # Step 1: Data Processing for Source A Residential Data
            node(
                func=cleanse_source_a_res,
                inputs="source_a_res_base",
                outputs="source_a_res_cleaned",
                name="cleanse_source_a_res",
            ),
            
            # Step 1.5: Column Selection for Source A Residential Data
            node(
                func=select_source_a_res_columns,
                inputs=["source_a_res_cleaned", "parameters"],
                outputs="source_a_res",
                name="select_source_a_res_columns",
            ),
            
            # Step 2: Data Processing for Source A Commercial Data
            node(
                func=cleanse_source_a_commercial,
                inputs="source_a_commercial_base",
                outputs="source_a_commercial_cleaned",
                name="cleanse_source_a_commercial",
            ),
            
            # Step 2.5: Column Selection for Source A OIR Data
            node(
                func=select_source_a_commercial_columns,
                inputs=["source_a_commercial_cleaned", "parameters"],
                outputs="source_a_commercial",
                name="select_source_a_commercial_columns",
            ),
            
            # Step 3: Data Processing for Source B Res Data
            node(
                func=cleanse_source_b_res,
                inputs="source_b_res_base", # Source B Res All Raw DB Data
                outputs="source_b_res_cleaned", # Source B Res Presentable Data
                name="cleanse_source_b_res"
            ),
            
            # Step 3.5: Column Selection for Source B Residential Data
            node(
                func=select_source_b_res_columns,
                inputs=["source_b_res_cleaned", "parameters"],
                outputs="source_b_res",
                name="select_source_b_res_columns",
            ),
            node(
                func=build_residential_quality_report,
                inputs=[
                    "raw_transaction_data",
                    "source_a_res_base",
                    "source_a_res",
                    "source_b_res_transactions",
                    "source_b_res_base",
                    "source_b_res",
                ],
                outputs="residential_quality_audit",
                name="build_residential_quality_report",
            ),
            
            # Step 4: Data Processing for Source B ICI Data
            node(
                func=cleanse_source_b_commercial,
                inputs=["source_b_commercial_base"],
                outputs="source_b_commercial_cleaned",
                name="cleanse_source_b_commercial"
            ),
            
            # Step 4.5: Column Selection for Source B ICI Data
            node(
                func=select_source_b_commercial_columns,
                inputs=["source_b_commercial_cleaned", "parameters"],
                outputs="source_b_commercial",
                name="select_source_b_commercial_columns",
            ),
            # Step 5: Combine all processed data and split by residential/commercial
            node(
                func=merge_and_excel,
                inputs=["source_a_res", "source_a_commercial", "source_b_res", "source_b_commercial", "parameters"],
                outputs={
                    'residential_2020_2023': 'output_residential_2020_2023',
                    'commercial_2020_2023': 'output_commercial_2020_2023',
                    'residential_2024_current': 'output_residential_2024_current',
                    'commercial_2024_current': 'output_commercial_2024_current'
                },
                name="merge_and_to_excel"
            )
        ]
    )

