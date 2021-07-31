from dataclasses import dataclass
from typing import List, Optional

from .identifier import DatasetIdentifier
from .version import DatasetVersion


@dataclass
class Dataset:
    identifier: DatasetIdentifier
    versions: List[DatasetVersion]

    @property
    def current_version(self) -> Optional[DatasetVersion]:
        if self.versions:
            return self.versions[-1]
        return None

    # def __post_init__(self):
    #     if isinstance(self.content, str):
    #         self.content = BytesIO(self.content.encode('utf-8'))
    #     elif isinstance(self.content, bytes):
    #         self.content = BytesIO(self.content)
    #     else:
    #         if not hasattr(self.content, 'read'):
    #             raise TypeError("Content doesn't provide a read method")
    #
    #
