from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline
from .pipelines.source_b_res import create_pipeline as source_b_res_pipeline
from .pipelines.source_a_res import create_pipeline as source_a_res_pipeline
from .pipelines.source_a_commercial import create_pipeline as source_a_commercial_pipeline
from .pipelines.source_b_commercial import create_pipeline as source_b_commercial_pipeline
from .pipelines.data_process import create_pipeline as data_process_pipeline

'''def register_pipelines():
    autodiscovered_pipelines = find_pipelines()
    pipelines = {**autodiscovered_pipelines}
    # Optionally define a default pipeline
    pipelines.update({"__default__": pipelines.get("source_b_res")})
    return pipelines'''
   
def register_pipelines():
    source_b_res = source_b_res_pipeline()
    source_b_commercial = source_b_commercial_pipeline()
    source_a_res = source_a_res_pipeline()
    source_a_commercial = source_a_commercial_pipeline()
    data_process = data_process_pipeline()
    
    return {
        "source_b_res": source_b_res,
        "source_b_commercial": source_b_commercial,
        "source_a_res": source_a_res,
        "source_a_commercial": source_a_commercial,
        "source_a": source_a_res + source_a_commercial,
        "source_b": source_b_res + source_b_commercial,
        "residential": source_b_res + source_a_res,
        "commercial": source_b_commercial + source_a_commercial,
        "data_process": data_process,
        "__all__": (source_b_res + source_b_commercial + source_a_res + source_a_commercial + data_process),
        "__default__": source_b_res + source_b_commercial + source_a_res + source_a_commercial + data_process,
    }
