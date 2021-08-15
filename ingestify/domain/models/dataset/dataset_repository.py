from abc import ABC, abstractmethod

from utils import ComponentFactory, ComponentRegistry

from .collection import DatasetCollection
from .dataset import Dataset
from .selector import Selector

dataset_repository_registry = ComponentRegistry()


class DatasetRepository(ABC, metaclass=dataset_repository_registry.metaclass):
    @abstractmethod
    def get_dataset_collection(
        self,
        dataset_type: str,
        provider: str,
        selector: Selector,
    ) -> DatasetCollection:
        pass

    @abstractmethod
    def save(self, dataset: Dataset):
        pass

    @abstractmethod
    def next_identity(self):
        pass

    @classmethod
    @abstractmethod
    def supports(cls, url: str) -> bool:
        pass


dataset_repository_factory = ComponentFactory.build_factory(
    DatasetRepository, dataset_repository_registry
)
