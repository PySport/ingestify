from abc import ABC, abstractmethod
from pathlib import Path
from typing import BinaryIO

from .dataset import Dataset
from ...services.identifier_key_transformer import IdentifierTransformer


class FileRepository(ABC):
    def __init__(self, url: str, identifier_transformer: IdentifierTransformer):
        self.base_dir = Path(url.split("://")[1])
        self.identifier_transformer = identifier_transformer

    def get_write_path(
        self, bucket: str, dataset: Dataset, revision_id: int, filename: str
    ) -> Path:
        # TODO: use the IdentifierKeyTransformer
        identifier_path = self.identifier_transformer.to_path(
            provider=dataset.provider,
            dataset_type=dataset.dataset_type,
            identifier=dataset.identifier,
        )

        path = (
            self.base_dir
            / bucket
            / f"provider={dataset.provider}"
            / f"dataset_type={dataset.dataset_type}"
            / identifier_path
            / str(revision_id)
            / filename
        )
        return path

    @abstractmethod
    def save_content(
        self,
        bucket: str,
        dataset: Dataset,
        revision_id: int,
        filename: str,
        stream: BinaryIO,
    ) -> Path:
        pass

    def get_read_path(self, storage_path: str) -> Path:
        return self.base_dir / storage_path

    @abstractmethod
    def load_content(self, storage_path: str) -> BinaryIO:
        pass

    @classmethod
    @abstractmethod
    def supports(cls, url: str) -> bool:
        pass

    def get_relative_path(self, path: Path) -> Path:
        """Return the relative path to the base of the repository"""
        return path.relative_to(self.base_dir)
