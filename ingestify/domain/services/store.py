from abc import abstractmethod, ABC
from typing import List

from domain.models import DatasetSelector, DatasetIdentifier, Dataset, DatasetVersion


class Store(ABC):
    @abstractmethod
    def get_dataset_identifiers(self, dataset_selector: DatasetSelector) -> List[DatasetIdentifier]:
        pass

    @abstractmethod
    def add(self, dataset: Dataset):
        pass
