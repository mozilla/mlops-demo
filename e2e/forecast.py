from starlette.requests import Request
from typing import Dict

import ray
from ray import serve
from ray.serve import Application
from metaflow import Flow, Run, get_metadata


@serve.deployment
class Forecaster:
    def __init__(self, args: Dict[str, str]):
        # The name of the flow (or the name + version) can be
        # provided via CLI or via config file.
        if "flow-name" not in args:
            raise Exception("Misconfigured server, missing 'flow-name' argument")

        print(f"Querying Metaflow metadata provider: {get_metadata()}")
        
        run = Run(f"{args['flow-name']}/{args['version']}")\
            if "version" in args\
            else Flow(args["flow-name"]).latest_successful_run
        
        print(f"Using Metaflow run: {str(run)} that finished trained on {run.finished_at}")

        self.args = args
        self.model = run.data.model

    def forecast(self, value: float) -> float:
        return self.model.predict([[value]])

    async def __call__(self, req: Request) -> str:
        # Extract the request 
        params = req.query_params
        if params is None or params.get("q") is None:
            raise Exception("Malformed query")

        return self.forecast(float(params.get("q")))


# Straight from this:
# https://docs.ray.io/en/latest/serve/advanced-guides/app-builder-guide.html#defining-an-application-builder
def app_builder(args: Dict[str, str]) -> Application:
    return Forecaster.bind(args)

# TODO Uncomment the next line if attempting to autogenerte the config.
# app = Forecaster.bind(args={})
