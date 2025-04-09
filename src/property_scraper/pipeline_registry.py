from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline
from .pipelines.midland_res import create_pipeline as midland_res_pipeline
from .pipelines.centaline_res import create_pipeline as centaline_res_pipeline

'''def register_pipelines():
    autodiscovered_pipelines = find_pipelines()
    pipelines = {**autodiscovered_pipelines}
    # Optionally define a default pipeline
    pipelines.update({"__default__": pipelines.get("midland_res")})
    return pipelines
