from typing import List

from .dataset import Dataset
from .identifier import Identifier


class DatasetCollection:
    def __init__(self, datasets: List[Dataset] = None):
        datasets = datasets or []

        self.datasets = {dataset.identifier.key: dataset for dataset in datasets}

    def get(self, dataset_identifier: Identifier) -> Dataset:
        return self.datasets.get(dataset_identifier.key)
