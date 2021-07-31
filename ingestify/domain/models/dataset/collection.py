from typing import List

from .dataset import Dataset
from .identifier import DatasetIdentifier


class DatasetCollection:
    def __init__(self, datasets: List[Dataset] = None):
        datasets = datasets or []

        self.datasets = {dataset.dataset_identifier: dataset for dataset in datasets}

    def get(self, dataset_identifier: DatasetIdentifier) -> Dataset:
        return self.datasets.get(dataset_identifier)
