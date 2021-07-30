from abc import abstractmethod, ABC
from typing import List

from domain.models import DatasetSelector, DatasetIdentifier, Dataset, DatasetVersion, DatasetCollection


class Store(ABC):
    @abstractmethod
    def get_dataset_collection(self, dataset_selector: DatasetSelector) -> DatasetCollection:
        pass

    @abstractmethod
    def add(self, dataset: Dataset):
        pass
