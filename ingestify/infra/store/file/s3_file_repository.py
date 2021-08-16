import os
import shutil
from pathlib import Path
from typing import IO, AnyStr

from ingestify.domain.models import Dataset, FileRepository


class S3FileRepository(FileRepository):
    def get_key(self, dataset: Dataset, version_id: int, filename: str) -> str:
        pass

    @classmethod
    def supports(cls, url: str) -> bool:
        return url.startswith("s3://")

    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)

    def save_content(self, file_id: str, stream: IO[AnyStr]):
        full_path = self.base_dir / file_id
        full_path.parent.mkdir(parents=True, exist_ok=True)

        with open(full_path, "wb") as fp:
            shutil.copyfileobj(stream, fp)

    def load_content(self, file_id: str) -> IO[AnyStr]:
        pass
