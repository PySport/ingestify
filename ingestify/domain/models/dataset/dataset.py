from dataclasses import dataclass

from .dataset_identifier import AbstractDatasetIdentifier
from .dataset_version import DatasetVersion


@dataclass
class Dataset:
    dataset_identifier: AbstractDatasetIdentifier
    dataset_version: DatasetVersion
