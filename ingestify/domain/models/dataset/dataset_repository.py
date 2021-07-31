from abc import ABC, abstractmethod

from domain.models import DatasetCollection, DatasetSelector


class DatasetRepository(ABC):
    @abstractmethod
    def get_dataset_collection(
        self, dataset_selector: DatasetSelector
    ) -> DatasetCollection:
        pass
