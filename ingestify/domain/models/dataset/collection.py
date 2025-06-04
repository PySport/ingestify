from typing import List, Optional

from .collection_metadata import DatasetCollectionMetadata
from .dataset import Dataset
from .identifier import Identifier


class DatasetCollection:
    def __init__(
        self,
        metadata: Optional[DatasetCollectionMetadata] = None,
        datasets: Optional[List[Dataset]] = None,
    ):
        datasets = datasets or []

        # TODO: this fails when datasets contains different dataset_types with overlapping identifiers
        self.datasets: dict[str, Dataset] = {
            dataset.identifier.key: dataset for dataset in datasets
        }
        self.metadata = metadata

    def get(self, dataset_identifier: Identifier) -> Dataset:
        return self.datasets.get(dataset_identifier.key)

    def __len__(self):
        return len(self.datasets)

    def __iter__(self):
        return iter(self.datasets.values())

    def first(self):
        try:
            return next(iter(self.datasets.values()))
        except StopIteration:
            raise Exception("No items in the collection")
