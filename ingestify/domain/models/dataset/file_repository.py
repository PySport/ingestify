from abc import ABC, abstractmethod
from typing import IO, AnyStr

from .dataset import Dataset


class FileRepository(ABC):
    @abstractmethod
    def save_content(self, file_key: str, stream: IO[AnyStr]):
        pass

    @abstractmethod
    def load_content(self, file_key: str) -> IO[AnyStr]:
        pass

    @abstractmethod
    def get_key(self, dataset: Dataset, version_id: int, filename: str) -> str:
        pass
