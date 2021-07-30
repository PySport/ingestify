from pathlib import Path
from typing import List

from domain.models import DatasetIdentifier, Dataset, DatasetSelector
from domain.services import Store


class LocalFileStore(Store):
    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)

    def get_dataset_identifiers(self, dataset_selector: DatasetSelector) -> List[DatasetIdentifier]:
        pass

    def add(self, dataset: Dataset):
        pass

