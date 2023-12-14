from abc import ABC, abstractmethod
from pathlib import Path
from typing import BinaryIO

from ingestify.utils import ComponentFactory, ComponentRegistry

from .dataset import Dataset

file_repository_registry = ComponentRegistry()


class FileRepository(ABC, metaclass=file_repository_registry.metaclass):
    def __init__(self, url: str):
        self.base_dir = Path(url.split("://")[1])

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

    @abstractmethod
    def load_content(
        self, bucket: str, dataset: Dataset, revision_id: int, filename: str
    ) -> BinaryIO:
        pass

    @classmethod
    @abstractmethod
    def supports(cls, url: str) -> bool:
        pass

    def get_path(
        self, bucket: str, dataset: Dataset, revision_id: int, filename: str
    ) -> Path:
        path = (
            self.base_dir
            / bucket
            / f"provider={dataset.provider}"
            / f"dataset_type={dataset.dataset_type}"
            / str(dataset.identifier)
            / str(revision_id)
            / filename
        )
        return path

    def get_relative_path(self, path: Path) -> Path:
        """Return the relative path to the base of the repository"""
        return path.relative_to(self.base_dir)


file_repository_factory = ComponentFactory.build_factory(
    FileRepository, file_repository_registry
)
