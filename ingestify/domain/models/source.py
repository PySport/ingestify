from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List

from .dataset import DatasetIdentifier
from .dataset_descriptor import BaseDatasetDescriptor
from .import_configuration import BaseImportConfiguration

from utils import ComponentFactory, ComponentRegistry

source_registry = ComponentRegistry()


@dataclass
class DatasetDescriptor:
    configuration: BaseImportConfiguration
    descriptor: BaseDatasetDescriptor

    @property
    def key(self):
        return ""


class Source(ABC, metaclass=source_registry.metaclass):
    @abstractmethod
    def fetch_dataset_identifiers(
        self, dataset_selector: DatasetIdentifier
    ) -> List[BaseDatasetDescriptor]:
        pass

    @abstractmethod
    def fetch_dataset(
        self, dataset_descriptor: BaseDatasetDescriptor, current_version: DatasetVersion
    ) -> None:
        pass


source_factory = ComponentFactory.build_factory(Source, source_registry)
