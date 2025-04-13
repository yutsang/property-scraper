from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline
from .pipelines.midland_res import create_pipeline as midland_res_pipeline
from .pipelines.centaline_res import create_pipeline as centaline_res_pipeline
from .pipelines.centaline_oir import create_pipeline as centaline_oir_pipeline
from .pipelines.midland_ici import create_pipeline as midland_ici_pipeline

'''def register_pipelines():
    autodiscovered_pipelines = find_pipelines()
    pipelines = {**autodiscovered_pipelines}
    # Optionally define a default pipeline
    pipelines.update({"__default__": pipelines.get("midland_res")})
    return pipelines'''
   
def register_pipelines():
    midland_res = midland_res_pipeline()
    midland_ici = midland_ici_pipeline()
    centaline_res = centaline_res_pipeline()
    centaline_oir = centaline_oir_pipeline()
    return {
        "midland_res": midland_res,
        "midland_ici": midland_ici,
        "centaline_res": centaline_res,
        "centaline_oir": centaline_oir,
        "centaline": centaline_res + centaline_oir,
        "midland": midland_res + midland_ici,
        "residential": midland_res + centaline_res,
        "commercial": midland_ici + centaline_oir,
        "__all__": (midland_res + midland_ici + centaline_res + centaline_oir),
        "__default__": midland_res + midland_ici + centaline_res + centaline_oir,
    }
