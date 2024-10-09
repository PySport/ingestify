from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Iterable, Iterator, Union

from .data_spec_version_collection import DataSpecVersionCollection
from .dataset.collection_metadata import DatasetCollectionMetadata
from .resources.dataset_resource import DatasetResource


class Source(ABC):
    def __init__(self, name: str, **kwargs):
        self.name = name

    @property
    @abstractmethod
    def provider(self) -> str:
        raise NotImplemented

    # TODO: consider making this required...
    # @abstractmethod
    # def discover_selectors(self, dataset_type: str) -> List[Dict]:
    #     pass

    @abstractmethod
    def find_datasets(
        self,
        dataset_type: str,
        data_spec_versions: DataSpecVersionCollection,
        dataset_collection_metadata: DatasetCollectionMetadata,
        **kwargs
    ) -> Iterator[List[DatasetResource]]:
        pass

    def __repr__(self):
        return self.__class__.__name__
