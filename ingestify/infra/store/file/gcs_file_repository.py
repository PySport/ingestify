from pathlib import Path
from typing import BinaryIO

from ingestify.domain import Dataset
from ingestify.domain.models import FileRepository


class GCSFileRepository(FileRepository):
    _client = None

    @property
    def client(self):
        if not self._client:
            from google.cloud import storage

            self._client = storage.Client()
        return self._client

    def __getstate__(self):
        return {
            "base_dir": self.base_dir,
            "_client": None,
            "identifier_transformer": self.identifier_transformer,
        }

    def save_content(
        self,
        bucket: str,
        dataset: Dataset,
        revision_id: int,
        filename: str,
        stream: BinaryIO,
    ) -> Path:
        key = self.get_write_path(bucket, dataset, revision_id, filename)
        gcs_bucket = key.parts[0]
        blob_name = str(Path(*key.parts[1:]))
        self.client.bucket(gcs_bucket).blob(blob_name).upload_from_file(stream)
        return key

    def load_content(self, storage_path: str) -> BinaryIO:
        key = self.get_read_path(storage_path)
        gcs_bucket = key.parts[0]
        blob_name = str(Path(*key.parts[1:]))
        blob = self.client.bucket(gcs_bucket).blob(blob_name)
        return blob.open("rb")

    @classmethod
    def supports(cls, url: str) -> bool:
        return url.startswith("gcs://")
