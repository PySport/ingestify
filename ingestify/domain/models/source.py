from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List

from .dataset_descriptor import BaseDatasetDescriptor
from .import_configuration import BaseImportConfiguration

from utils import ComponentFactory, ComponentRegistry
from ..services import Store

source_registry = ComponentRegistry()


@dataclass
class DatasetDescriptor:
    configuration: BaseImportConfiguration
    descriptor: BaseDatasetDescriptor

    @property
    def key(self):
        return ""


class Source(ABC, metaclass=source_registry.metaclass):
    def __init__(self, version_policy: RefreshPolicy):
        self.refresh_policy = refresh_policy

    @abstractmethod
    async def find_datasets(
        self, configuration: BaseImportConfiguration
    ) -> List[BaseDatasetDescriptor]:
        pass

    @abstractmethod
    async def retrieve_and_store_dataset(self, dataset: BaseDatasetDescriptor, store: Store) -> None:
        pass


source_factory = ComponentFactory.build_factory(Source, source_registry)
