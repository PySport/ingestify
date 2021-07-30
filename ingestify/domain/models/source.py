from abc import ABC, abstractmethod
from typing import List

from .dataset import DatasetIdentifier, DatasetVersion, DatasetSelector

from utils import ComponentFactory, ComponentRegistry

source_registry = ComponentRegistry()


class Source(ABC, metaclass=source_registry.metaclass):
    @abstractmethod
    def fetch_dataset_identifiers(
        self, dataset_selector: DatasetSelector
    ) -> List[DatasetIdentifier]:
        pass

    @abstractmethod
    def fetch_dataset(
        self, dataset_identifier: DatasetIdentifier, current_version: DatasetVersion
    ) -> None:
        pass


source_factory = ComponentFactory.build_factory(Source, source_registry)
