import os
from metaflow import current
import logging
import sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

class ArtifactStore:
    def get_storage_path(self, filename: str) -> str:
        return f"{current.flow_name}/{current.run_id}/{filename}"

    def store(self, data: bytes, filename: str) -> str:
        from google.cloud import storage

        client = storage.Client(project="moz-fx-mlops-inference-nonprod")
        bucket = client.get_bucket("mf-models-test1")

        self.deployment_path = self.get_storage_path(filename)

        blob = bucket.blob(self.deployment_path)

        with io.BytesIO(data) as f:
            # TODO: Catch exceptions and report back.

            # Google recommends setting `if_generation_match=0` if the
            # object is expected to be new. We don't expect collisions,
            # so setting this to 0 seems good.
            blob.upload_from_file(f, if_generation_match=0)
            logging.info(f"The model is stored at {self.deployment_path}")

        return self.deployment_path
