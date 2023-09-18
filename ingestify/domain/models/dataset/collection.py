from typing import List, Optional, TYPE_CHECKING

from .dataset import Dataset
from .identifier import Identifier

if TYPE_CHECKING:
    from ingestify.application.dataset_store import DatasetStore


class DatasetCollection:
    def __init__(self, datasets: List[Dataset] = None):
        self.store: Optional["DatasetStore"] = None
        datasets = datasets or []

        self.datasets: dict[str, Dataset] = {
            dataset.identifier.key: dataset for dataset in datasets
        }

    def set_store(self, store: "DatasetStore"):
        self.store = store
        for dataset in self.datasets.values():
            dataset.set_store(store)

    def get(self, dataset_identifier: Identifier) -> Dataset:
        return self.datasets.get(dataset_identifier.key)

    def __len__(self):
        return len(self.datasets)

    def __iter__(self):
        return iter(self.datasets.values())

    def get_dataset_by_id(self, dataset_id):
        for dataset in self:
            if dataset.dataset_id == dataset_id:
                return dataset
        return None

    def first(self):
        try:
            return next(iter(self.datasets.values()))
        except StopIteration:
            raise Exception("No items in the collection")
