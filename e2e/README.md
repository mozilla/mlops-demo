# End to end example

> [!IMPORTANT]
> Python version >= 3.10 is required. It is recommended to install the requirements
> in a Python Virtual Environment and execute the following workflows in such
> virtual environment.
> **The workflows expects user to have Weights and Biases and Outerbounds Metaflow accounts configured.**
> In order to have both of those accounts configuerd, you'll need the help of an MLOps admin. Please reach out to us via the #mlops channel in Slack, and we can help you get those accounts set up.

## Train a model

1. From the e2e directory of this repository, run `python3.10 -m pip install -r requirements.txt` (or substitute in `python -m` if Python 3.10 is your default distribution).
2. Try to run the training flow locally with `python training-flow.py --metadata=local --environment=pypi run --offline True`.
3. To record the data in Weights and Biases, the `WANDB_API_KEY`, `WANDB_ENTITY` and `WANDB_PROJECT` environment variables need to be set. You can set these locally in your virtual envuronment via the command line, or if you're using CI, you can set them on CI. Then you can run this command: `python training-flow.py --environment=pypi run --with kubernetes`.
5. (_Optional_) To run the training on a cluster without recording informations to W&B, use the following command: `python training-flow.py --environment=pypi run --offline True --with kubernetes`

You can track the training progress on the Outerbounds UI.

## Stand up an example inference server

1. From the e2e directory, run `pip install -r requirements.txt`
2. Try the inference server locally: `serve run forecast:app_builder flow-name=HelloFlowBQ namespace=<MODEL NAMESPACE>` where
`<MODEL NAMESPACE>` is the namespace used to store the model in Metaflow, e.g. `user:aplacitelli@mozilla.com`.

> [!NOTE]
> The previous steps follow Ray Serve [Local Development with HTTP requests](https://docs.ray.io/en/latest/serve/advanced-guides/dev-workflow.html#local-development-with-http-requests) workflow, allowing fast
> local iteration to prototype the inference server. The next steps are useful to test
> a deployment process similar to the one happening in production, but not strictly
> required for local development.

3. Autogenerate the config file via: `serve build forecast:app -o serve_config.yaml`. Note that
we reference `app` and not `app_builder` here because of a bug in the generator. It would complain
with `TypeError: Expected 'forecast:app_builder' to be an Application but got <class 'function'>.` otherwise.
4. Tweak the config `serve_config.yaml`, especially the `applications` section. Here's a sample tweaked:

```yaml
# This file was generated using the `serve build` command on Ray v2.9.3.
proxy_location: EveryNode

http_options:
  host: 0.0.0.0
  port: 8000

grpc_options:
  port: 9000
  grpc_servicer_functions: []

logging_config:
  encoding: TEXT
  log_level: INFO
  logs_dir: null
  enable_access_log: true

applications:
- name: ray-flow1
  route_prefix: /
  import_path: forecast:app_builder
  args:
    namespace: "user:CHANGEME" # TODO, change this!
    flow-name: HelloFlowBQ
    version: 28
  runtime_env:
    pip:
      - outerbounds[gcp]
      - scikit-learn==1.3.1
  deployments:
  - name: Forecaster
```

5. (_Optional_) Start a local ray cluster: `ray start --head`.
6. Deploy the server: `serve deploy serve_config.yaml`.
7. (_Optional_) Check the status via `serve status`.

## Testing the model
Try the model: [`curl http://127.0.0.1:8000/?q=1.4`](http://127.0.0.1:8000/?q=1.4).
You can also visit that URL in your browser. 
