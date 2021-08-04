from abc import ABC, abstractmethod

from .collection import DatasetCollection
from .dataset import Dataset
from .selector import DatasetSelector


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
