from abc import ABC, abstractmethod
from typing import List, Optional, Union, Dict

from utils import ComponentFactory, ComponentRegistry
from . import DraftFile

from .dataset import Identifier, Selector, Version

source_registry = ComponentRegistry()


class Source(ABC, metaclass=source_registry.metaclass):
    @property
    @abstractmethod
    def dataset_type(self) -> str:
        pass

    @property
    @abstractmethod
    def provider(self) -> str:
        pass

    @abstractmethod
    def discover_datasets(
        self, selector: Selector
    ) -> List[Identifier]:
        pass

    @abstractmethod
    def fetch_dataset_files(
        self,
        dataset_identifier: Identifier,
        current_version: Optional[Version],
    ) -> Dict[str, Optional[DraftFile]]:
        pass

    def __repr__(self):
        return self.__class__.__name__


source_factory = ComponentFactory.build_factory(Source, source_registry)
