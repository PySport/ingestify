import os
import shutil
from pathlib import Path
from typing import IO, AnyStr, BinaryIO
import io

import requests

from ingestify.domain.models import Dataset, FileRepository


class HTTPFileRepository(FileRepository):
    @classmethod
    def supports(cls, url: str) -> bool:
        return url.startswith("https://") or url.startswith("http://")

    def __init__(self, url: str):
        self.url = url

    def save_content(
        self,
        bucket: str,
        dataset: Dataset,
        version_id: int,
        filename: str,
        stream: BinaryIO,
    ):
        url = self._get_url(bucket, dataset, version_id, filename)

        response = requests.put(url, data=stream)
        response.raise_for_status()

    def load_content(
        self, bucket: str, dataset: Dataset, version_id: int, filename: str
    ) -> IO[AnyStr]:
        fp = io.BytesIO()

        url = self._get_url(bucket, dataset, version_id, filename)

        with requests.get(url, stream=True) as response:
            response.raise_for_status()

            # TODO: return a IO instead of writing to memory first
            for chunk in response.iter_content(chunk_size=8192):
                fp.write(chunk)
        fp.seek(0)
        return fp

    def _get_url(
        self, bucket: str, dataset: Dataset, version_id: int, filename: str
    ) -> str:
        return (
            self.url + f"/buckets/{bucket}/datasets"
            f"/{dataset.dataset_id}/files/{version_id}/{filename}"
        )
