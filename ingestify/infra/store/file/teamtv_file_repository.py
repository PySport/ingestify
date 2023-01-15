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
        self.resource_group = url[9:]
        self.http_repository = HTTPFileRepository(
            url="http://127.0.0.1:8080/api"
        )

    def save_content(self, bucket: str, dataset: Dataset, version_id: int, filename: str, stream: BinaryIO):
        self.http_repository.save_content(
            bucket,
            dataset,
            version_id,
            filename,
            stream
        )

    def load_content(self, bucket: str, dataset: Dataset, version_id: int, filename: str) -> IO[AnyStr]:
        return self.http_repository.load_content(
            bucket,
            dataset,
            version_id,
            filename
        )
