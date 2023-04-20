import os
import shutil
from pathlib import Path
from typing import IO, AnyStr

from ingestify.domain.models import Dataset, FileRepository


class LocalFileRepository(FileRepository):
    @classmethod
    def supports(cls, url: str) -> bool:
        return url.startswith("file://")

    def __init__(self, url: str):
        self.base_dir = Path(url[7:])

    def save_content(
        self,
        bucket: str,
        dataset: Dataset,
        version_id: int,
        filename: str,
        stream: IO[AnyStr],
    ):
        full_path = self._get_path(bucket, dataset, version_id, filename)
        full_path.parent.mkdir(parents=True, exist_ok=True)

        with open(full_path, "wb") as fp:
            shutil.copyfileobj(stream, fp)

    def load_content(
        self, bucket: str, dataset: Dataset, version_id: int, filename: str
    ) -> IO[AnyStr]:
        return open(self._get_path(bucket, dataset, version_id, filename), "rb")

    def _get_path(
        self, bucket: str, dataset: Dataset, version_id: int, filename: str
    ) -> Path:
        return (
            self.base_dir
            / bucket
            / Path(dataset.dataset_id)
            / str(version_id)
            / filename
        )
