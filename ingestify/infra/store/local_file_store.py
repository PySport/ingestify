from pathlib import Path

from domain.models import Dataset, DatasetSelector, DatasetCollection
from domain.services import Store


class LocalFileStore(Store):
    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)

    def get_dataset_collection(self, dataset_selector: DatasetSelector) -> DatasetCollection:
        return DatasetCollection()

    def add(self, dataset: Dataset):
        dataset_path = self.base_dir / dataset.identifier.key
        dataset_path.mkdir(parents=True, exist_ok=True)

        with open(dataset_path / "content", "wb") as fp:
            fp.write(dataset.content.read())

