from abc import abstractmethod, ABC
from typing import List

from domain.models import DatasetSelector, DatasetIdentifier, Dataset, DatasetVersion, DatasetCollection


class Store:
    def get_dataset_collection(self, dataset_selector: DatasetSelector) -> DatasetCollection:
        pass

    def add_version(self, dataset: Dataset, version: DraftDatasetVersion):
        pass
