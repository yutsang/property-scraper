from kedro.pipeline import Pipeline, node, pipeline
from .nodes import cleanse_centaline_res, cleanse_centaline_oir
from .nodes import cleanse_midland_ici, cleanse_midland_res
from .nodes import merge_and_excel

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
                outputs="centaline_res",
                name="cleanse_centaline_res",
            ),
            
            # Step 2: Data Processing for Centaline Commercial Data
            node(
                func=cleanse_centaline_oir,
                inputs="centaline_oir_base",
                outputs="centaline_oir",
                name="cleanse_centaline_oir",
            ),
            
            # Step 3: Data Processing for Midland Res Data
            node(
                func=cleanse_midland_res,
                inputs="midland_res_base", # Midland Res All Raw DB Data
                outputs="midland_res", # Midland Res Presentable Data
                name="cleanse_midland_res"
            ),
            # Step 4: Data Processing for Midland ICI Data
            node(
                func=cleanse_midland_ici,
                inputs=["midland_ici_base"],
                outputs="midland_ici",
                name="cleanse_midland_ici"
            ),
            # Step 5: Combine all processed data
            node(
                func=merge_and_excel,
                inputs=["centaline_res", "centaline_oir", "midland_res", "midland_ici"],
                outputs={
                'excel_2020_2022': 'output_excel_2020_2022',
                'excel_2023_current': 'output_excel_2023_current'
                },
                name="merge_and_to_excel"
            )
        ]
    )

