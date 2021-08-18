from abc import ABC, abstractmethod
from typing import IO, AnyStr, BinaryIO

from ingestify.utils import ComponentFactory, ComponentRegistry

from .dataset import Dataset

file_repository_registry = ComponentRegistry()


class FileRepository(ABC, metaclass=file_repository_registry.metaclass):
    @abstractmethod
    def save_content(self, file_key: str, stream: BinaryIO):
        pass

    @abstractmethod
    def load_content(self, file_key: str) -> BinaryIO:
        pass

    @abstractmethod
    def get_key(self, dataset: Dataset, version_id: int, filename: str) -> str:
        pass

    @classmethod
    @abstractmethod
    def supports(cls, url: str) -> bool:
        pass


file_repository_factory = ComponentFactory.build_factory(
    FileRepository, file_repository_registry
)
