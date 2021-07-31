from abc import ABC, abstractmethod
from typing import List, Optional

from utils import ComponentFactory, ComponentRegistry

from .dataset import DatasetIdentifier, DatasetSelector, DatasetVersion, DraftDatasetVersion

source_registry = ComponentRegistry()


class Source(ABC, metaclass=source_registry.metaclass):
    @abstractmethod
    def fetch_dataset_identifiers(
        self, dataset_selector: DatasetSelector
    ) -> List[DatasetIdentifier]:
        pass

    @abstractmethod
    def fetch_draft_dataset_version(
        self,
        dataset_identifier: DatasetIdentifier,
        current_version: Optional[DatasetVersion],
    ) -> DraftDatasetVersion:
        pass


source_factory = ComponentFactory.build_factory(Source, source_registry)
