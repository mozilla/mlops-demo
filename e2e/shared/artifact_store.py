import os
import io
from metaflow import current
import logging
import sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

class ArtifactStore:
    def _get_storage_path(self, filename: str) -> str:
        return f"{current.flow_name}/{current.run_id}/{filename}"

    def _internal_store(self, data: bytes, storage_path: str) -> str:
        from google.cloud import storage

        client = storage.Client(project="moz-fx-mlops-inference-nonprod")
        bucket = client.get_bucket("mf-models-test1")

        blob = bucket.blob(storage_path)

        with io.BytesIO(data) as f:
            # TODO: Catch exceptions and report back.

            # Google recommends setting `if_generation_match=0` if the
            # object is expected to be new. We don't expect collisions,
            # so setting this to 0 seems good.
            blob.upload_from_file(f, if_generation_match=0)
            logging.info(f"The model is stored at {storage_path}")

    def store(self, data: bytes, filename: str) -> str:
        self.deployment_path = self._get_storage_path(filename)

        self._internal_store(data, self.deployment_path)

        return self.deployment_path

if __name__ == "__main__":
    test = ArtifactStore()
    test._internal_store(b"test", "mf-models-test1/remove-me/test-2/itworks.txt")
