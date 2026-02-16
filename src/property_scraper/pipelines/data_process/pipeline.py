from kedro.pipeline import Pipeline, node, pipeline
from .nodes import cleanse_centaline_res, cleanse_centaline_oir
from .nodes import cleanse_midland_ici, cleanse_midland_res
from .nodes import merge_and_excel
from .nodes import select_centaline_res_columns, select_centaline_oir_columns
from .nodes import select_midland_res_columns, select_midland_ici_columns

def create_pipeline(**kwargs) -> Pipeline:
    """
    Create the midland area codes pipeline
    
    Returns:
        Pipeline: A pipeline to fetch district codes
    """

    return Pipeline(
        [
            # Step 1: Data Processing for Centaline Residential Data
            node(
                func=cleanse_centaline_res,
                inputs="centaline_res_base",
                outputs="centaline_res_cleaned",
                name="cleanse_centaline_res",
            ),
            
            # Step 1.5: Column Selection for Centaline Residential Data
            node(
                func=select_centaline_res_columns,
                inputs="centaline_res_cleaned",
                outputs="centaline_res",
                name="select_centaline_res_columns",
            ),
            
            # Step 2: Data Processing for Centaline Commercial Data
            node(
                func=cleanse_centaline_oir,
                inputs="centaline_oir_base",
                outputs="centaline_oir_cleaned",
                name="cleanse_centaline_oir",
            ),
            
            # Step 2.5: Column Selection for Centaline OIR Data
            node(
                func=select_centaline_oir_columns,
                inputs="centaline_oir_cleaned",
                outputs="centaline_oir",
                name="select_centaline_oir_columns",
            ),
            
            # Step 3: Data Processing for Midland Res Data
            node(
                func=cleanse_midland_res,
                inputs="midland_res_base", # Midland Res All Raw DB Data
                outputs="midland_res_cleaned", # Midland Res Presentable Data
                name="cleanse_midland_res"
            ),
            
            # Step 3.5: Column Selection for Midland Residential Data
            node(
                func=select_midland_res_columns,
                inputs="midland_res_cleaned",
                outputs="midland_res",
                name="select_midland_res_columns",
            ),
            
            # Step 4: Data Processing for Midland ICI Data
            node(
                func=cleanse_midland_ici,
                inputs=["midland_ici_base"],
                outputs="midland_ici_cleaned",
                name="cleanse_midland_ici"
            ),
            
            # Step 4.5: Column Selection for Midland ICI Data
            node(
                func=select_midland_ici_columns,
                inputs="midland_ici_cleaned",
                outputs="midland_ici",
                name="select_midland_ici_columns",
            ),
            # Step 5: Combine all processed data and split by residential/commercial
            node(
                func=merge_and_excel,
                inputs=["centaline_res", "centaline_oir", "midland_res", "midland_ici"],
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

