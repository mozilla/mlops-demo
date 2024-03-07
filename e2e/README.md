# Example server

> [!NOTE]
> Python version >= 3.10 is required.

1. `pip install -r requirements.txt`
2. Try if that works locally: `serve run forecast:app_builder flow-name=HelloFlowBQ`
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
8. Try the model: `curl http://127.0.0.1:8000/?q=1.4`.
