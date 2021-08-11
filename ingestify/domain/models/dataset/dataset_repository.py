from abc import ABC, abstractmethod

from .collection import DatasetCollection
from .dataset import Dataset
from .selector import Selector


class DatasetRepository(ABC):
    @abstractmethod
    def get_dataset_collection(
        self, dataset_type: str, provider: str, selector: Selector,
    ) -> DatasetCollection:
        pass

    @abstractmethod
    def save(self, dataset: Dataset):
        pass

    @abstractmethod
    def next_identity(self):
        pass
