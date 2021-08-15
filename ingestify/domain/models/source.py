from abc import ABC, abstractmethod
from typing import Dict, List, Optional

from utils import ComponentFactory, ComponentRegistry

from . import DraftFile
from .dataset import Identifier, Version

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
        self, **kwargs
    ) -> List[Dict]:
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
