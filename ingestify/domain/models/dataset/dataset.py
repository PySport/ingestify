from dataclasses import dataclass

from .identifier import DatasetIdentifier
from .version import DatasetVersion


@dataclass
class Dataset:
    dataset_identifier: DatasetIdentifier
    dataset_version: DatasetVersion
