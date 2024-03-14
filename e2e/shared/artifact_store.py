import os
import io
import logging
import sys

from pathlib import Path

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

PROJECT_NAME = "moz-fx-mlops-inference-nonprod"
BUCKET_NAME = "mf-models-test1"

class ArtifactStore:
    def _get_storage_path(self,flow_name: str, run_id: str, file_name: str) -> str:
        return f"{flow_name}/{run_id}/{file_name}"

    def _internal_store(self, data: bytes, storage_path: str) -> str:
        from google.cloud import storage

        client = storage.Client(project=PROJECT_NAME)
        bucket = client.get_bucket(BUCKET_NAME)

        blob = bucket.blob(storage_path)

        with io.BytesIO(data) as f:
            # TODO: Catch exceptions and report back.

            # Google recommends setting `if_generation_match=0` if the
            # object is expected to be new. We don't expect collisions,
            # so setting this to 0 seems good.
            blob.upload_from_file(f, if_generation_match=0)
            logging.info(f"The model is stored at {storage_path}")

    def store(self, data: bytes, filename: str) -> str:
        from metaflow import current
        
        self.deployment_path = self._get_storage_path(
            current.flow_name, current.run_id, filename
        )

        self._internal_store(data, self.deployment_path)

        return self.deployment_path

    def fetch(self, flow_name: str, run_id: str, file_name: str) -> str:
        from google.cloud import storage

        path = self._get_storage_path(
            flow_name=flow_name, run_id=run_id, file_name=file_name
        )

        client = storage.Client(project=PROJECT_NAME)
        bucket = client.get_bucket(BUCKET_NAME)

        blob = bucket.blob(path)

        # Create any directory that's needed.
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)

        blob.download_to_filename(path)

        return path
