from kedro.framework.project import find_pipelines

def register_pipelines():
    autodiscovered_pipelines = find_pipelines()
    pipelines = {**autodiscovered_pipelines}
    # Optionally define a default pipeline
    pipelines.update({"__default__": pipelines.get("midland_res")})
    return pipelines
