from abc import ABC, abstractmethod
from typing import IO, AnyStr

from .dataset import Dataset
from .version import DatasetVersion


class FileRepository(ABC):
    @abstractmethod
    def save_content(self, file_id: str, stream: IO[AnyStr]):
        pass

    @abstractmethod
    def load_content(self, file_id: str) -> IO[AnyStr]:
        pass

    @abstractmethod
    def get_identify(self, dataset: Dataset, version_id: str, filename: str) -> str:
        pass
