import gzip
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import IO, AnyStr, BinaryIO

from ingestify.utils import ComponentFactory, ComponentRegistry

from .dataset import Dataset

file_repository_registry = ComponentRegistry()


class FileRepository(ABC, metaclass=file_repository_registry.metaclass):
    def __init__(self, url: str):
        self.base_dir = Path(url.split("://")[1])

    def prepare_write_stream(self, stream: BinaryIO) -> BinaryIO:
        if self.compression == "gzip":
            output_stream = BinaryIO()

            with gzip.GzipFile(fileobj=output_stream, compresslevel=9, mode="wb") as fp:
                shutil.copyfileobj(stream, fp)
            output_stream.seek(0)
        else:
            output_stream = stream

        return output_stream

    def prepare_read_stream(self, stream: BinaryIO) -> BinaryIO:
        pass

    @abstractmethod
    def save_content(
        self,
        bucket: str,
        dataset: Dataset,
        version_id: int,
        filename: str,
        stream: BinaryIO,
    ) -> Path:
        pass

    @abstractmethod
    def load_content(
        self, bucket: str, dataset: Dataset, version_id: int, filename: str
    ) -> BinaryIO:
        pass

    @classmethod
    @abstractmethod
    def supports(cls, url: str) -> bool:
        pass

    def get_path(
        self, bucket: str, dataset: Dataset, version_id: int, filename: str
    ) -> Path:
        path = (
            self.base_dir
            / bucket
            / f"provider={dataset.provider}"
            / f"dataset_type={dataset.dataset_type}"
            / str(dataset.identifier)
            / str(version_id)
            / filename
        )
        return path


file_repository_factory = ComponentFactory.build_factory(
    FileRepository, file_repository_registry
)
