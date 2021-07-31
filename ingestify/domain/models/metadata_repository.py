from abc import ABC, abstractmethod

from domain.models import DatasetSelector, DatasetCollection


class MetadataRepository(ABC):
    @abstractmethod
    def get_dataset_collection(self, dataset_selector: DatasetSelector) -> DatasetCollection:
        pass
