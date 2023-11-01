from abc import ABC, abstractmethod
from typing import Dict, List, Optional

# from ingestify.utils import ComponentFactory, ComponentRegistry

from . import DraftFile
from .dataset import Identifier, Version


class Source(ABC):
    def __init__(self, name: str, **kwargs):
        self.name = name

    @property
    @abstractmethod
    def provider(self) -> str:
        pass

    # TODO: consider making this required...
    # @abstractmethod
    # def discover_selectors(self, dataset_type: str) -> List[Dict]:
    #     pass

    @abstractmethod
    def discover_datasets(self, dataset_type: str, **kwargs) -> List[Dict]:
        pass

    @abstractmethod
    def fetch_dataset_files(
        self,
        dataset_type: str,
        identifier: Identifier,
        current_version: Optional[Version],
    ) -> Dict[str, Optional[DraftFile]]:
        pass

    def __repr__(self):
        return self.__class__.__name__
