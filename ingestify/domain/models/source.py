from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Iterable, Iterator, Union

# from ingestify.utils import ComponentFactory, ComponentRegistry

from . import DraftFile
from .data_spec_version_collection import DataSpecVersionCollection
from .dataset import Identifier, Revision


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
    def discover_datasets(
        self, dataset_type: str, data_spec_versions: DataSpecVersionCollection, **kwargs
    ) -> Union[List[Dict], Iterator[List[Dict]]]:
        pass

    @abstractmethod
    def fetch_dataset_files(
        self,
        dataset_type: str,
        identifier: Identifier,
        data_spec_versions: DataSpecVersionCollection,
        current_revision: Optional[Revision],
    ) -> Dict[str, Optional[DraftFile]]:
        pass

    def __repr__(self):
        return self.__class__.__name__
