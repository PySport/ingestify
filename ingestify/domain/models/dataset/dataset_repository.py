from abc import ABC, abstractmethod
from typing import Optional, List, Union

from .collection import DatasetCollection
from .dataset import Dataset
from .dataset_state import DatasetState
from .selector import Selector


class DatasetRepository(ABC):
    @abstractmethod
    def get_dataset_collection(
        self,
        bucket: str,
        dataset_type: Optional[str] = None,
        dataset_id: Optional[Union[str, List[str]]] = None,
        provider: Optional[str] = None,
        selector: Optional[Union[Selector, List[Selector]]] = None,
        metadata_only: bool = False,
        dataset_state: Optional[List[DatasetState]] = None,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
    ) -> DatasetCollection:
        pass

    @abstractmethod
    def destroy(self, dataset: Dataset):
        pass

    @abstractmethod
    def save(self, bucket: str, dataset: Dataset):
        pass

    @abstractmethod
    def next_identity(self):
        pass
