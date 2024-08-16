from abc import ABC, abstractmethod
from typing import Optional, List, Union

from ingestify.utils import ComponentFactory, ComponentRegistry

from .collection import DatasetCollection
from .dataset import Dataset
from .selector import Selector

dataset_repository_registry = ComponentRegistry()


class DatasetRepository(ABC, metaclass=dataset_repository_registry.metaclass):
    @abstractmethod
    def get_dataset_collection(
        self,
        bucket: str,
        dataset_type: Optional[str] = None,
        dataset_id: Optional[Union[str, List[str]]] = None,
        provider: Optional[str] = None,
        selector: Optional[Union[Selector, List[Selector]]] = None,
        metadata_only: bool = False,
    ) -> DatasetCollection:
        pass

    @abstractmethod
    def destroy(self, dataset: Dataset):
        pass

    @abstractmethod
    def save(self, bucket: str, dataset: Dataset):
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
