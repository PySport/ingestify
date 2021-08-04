from abc import ABC, abstractmethod

from .selector import DatasetSelector
from .collection import DatasetCollection
from .dataset import Dataset


class DatasetRepository(ABC):
    @abstractmethod
    def get_dataset_collection(
        self, dataset_selector: DatasetSelector
    ) -> DatasetCollection:
        pass

    @abstractmethod
    def save(self, dataset: Dataset):
        pass

    @abstractmethod
    def next_identity(self):
        pass
