from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List

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
    async def determine_datasets(
        self, configuration: BaseImportConfiguration
    ) -> List[BaseDatasetDescriptor]:
        pass

    @abstractmethod
    async def fetch(self, dataset: BaseDatasetDescriptor) -> None:
        pass


source_factory = ComponentFactory.build_factory(Source, source_registry)
