from abc import abstractmethod, ABC

from .file import File


class FileRepository(ABC):
    @abstractmethod
    def save(self, file: File):
        pass

    @abstractmethod
    def load(self, file_id: str) -> File:
        pass

    @abstractmethod
    def get_identify(self) -> str:
        pass
