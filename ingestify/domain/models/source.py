from abc import ABC, abstractmethod
from typing import List, Optional

from utils import ComponentFactory, ComponentRegistry

from .dataset import DatasetIdentifier, DatasetSelector, DatasetVersion

source_registry = ComponentRegistry()


class Source(ABC, metaclass=source_registry.metaclass):
    @abstractmethod
    def discover_datasets(
        self, dataset_selector: DatasetSelector
    ) -> List[DatasetIdentifier]:
        pass

    @abstractmethod
    def fetch_dataset_files(
        self,
        dataset_identifier: DatasetIdentifier,
        current_version: Optional[DatasetVersion],
    ) -> DatasetVersion:
        pass


source_factory = ComponentFactory.build_factory(Source, source_registry)
