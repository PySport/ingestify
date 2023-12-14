import os
import shutil
from pathlib import Path
from typing import IO, AnyStr, BinaryIO

from ingestify.domain.models import Dataset, FileRepository


class LocalFileRepository(FileRepository):
    @classmethod
    def supports(cls, url: str) -> bool:
        return url.startswith("file://")

    def save_content(
        self,
        bucket: str,
        dataset: Dataset,
        revision_id: int,
        filename: str,
        stream: BinaryIO,
    ) -> Path:
        path = self.get_path(bucket, dataset, revision_id, filename)
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "wb") as fp:
            shutil.copyfileobj(stream, fp)
        return path

    def load_content(
        self, bucket: str, dataset: Dataset, revision_id: int, filename: str
    ) -> BinaryIO:
        return open(self.get_path(bucket, dataset, revision_id, filename), "rb")
