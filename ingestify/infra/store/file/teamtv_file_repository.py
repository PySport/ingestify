import os
import shutil
from pathlib import Path
from typing import IO, AnyStr, BinaryIO

from ingestify.domain.models import Dataset, FileRepository
from ingestify.infra.store.file.http_file_repository import HTTPFileRepository


class TeamTVFileRepository(FileRepository):
    @classmethod
    def supports(cls, url: str) -> bool:
        return url.startswith("teamtv://")

    def __init__(self, url: str):
        self.bucket = url[9:]
        self.http_repository = HTTPFileRepository(
            url=f"http://127.0.0.1:8080/api/buckets/{self.bucket}/datasets" +
                "/{dataset_id}/files/{version_id}/{filename}"
        )

    def save_content(self, dataset: Dataset, version_id: int, filename: str, stream: BinaryIO):
        self.http_repository.save_content(
            dataset, version_id, filename, stream
        )

    def load_content(self, dataset: Dataset, version_id: int, filename: str) -> IO[AnyStr]:
        return self.http_repository.load_content(
            dataset,
            version_id,
            filename
        )
