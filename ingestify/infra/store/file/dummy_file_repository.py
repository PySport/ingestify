import logging
from pathlib import Path
from typing import BinaryIO

from ingestify.domain.models import Dataset, FileRepository


logger = logging.getLogger(__name__)


class DummyFileRepository(FileRepository):
    @classmethod
    def supports(cls, url: str) -> bool:
        return url.startswith("dummy://")

    def save_content(
        self,
        bucket: str,
        dataset: Dataset,
        revision_id: int,
        filename: str,
        stream: BinaryIO,
    ) -> Path:
        path = self.get_write_path(bucket, dataset, revision_id, filename)

        logger.info(f"Dummy save content to {path}")

        return path

    def load_content(self, storage_path: str) -> BinaryIO:
        return BinaryIO()
