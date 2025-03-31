from kedro.pipeline import Pipeline, node
from .nodes import scrape_estates

def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline([
        node(
            func=scrape_estates,
            inputs=["centanet_area_codes", "params:base_url"],
            outputs="centanet_estates",
            name="scrape_estates_node"
        )
    ])
