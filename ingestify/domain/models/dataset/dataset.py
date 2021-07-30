from abc import ABC, abstractmethod
from dataclasses import dataclass
from io import BytesIO
from typing import AnyStr, IO, Union

from .identifier import DatasetIdentifier
from .version import DatasetVersion


class Content(ABC):
    @abstractmethod
    def read(self, n: int = -1) -> AnyStr:
        pass


@dataclass
class Dataset:
    identifier: DatasetIdentifier
    version: DatasetVersion

    content: Union[AnyStr, BytesIO, Content]

    def __post_init__(self):
        if isinstance(self.content, str):
            self.content = BytesIO(self.content.encode('utf-8'))
        elif isinstance(self.content, bytes):
            self.content = BytesIO(self.content)
        else:
            if not hasattr(self.content, 'read'):
                raise TypeError("Content doesn't provide a read method")


