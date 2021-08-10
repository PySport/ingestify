import glob
import os
import pickle
import uuid
from pathlib import Path

from domain.models import (Dataset, DatasetCollection, DatasetRepository,
                           Selector)


def parse_value(v):
    try:
        return int(v)
    except ValueError:
        return v


class LocalDatasetRepository(DatasetRepository):
    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)

    def get_dataset_collection(
        self, selector: Selector
    ) -> DatasetCollection:

        datasets = []
        for dir_name in glob.glob(str(self.base_dir / "*")):
            attributes = {
                item[0]: parse_value(item[1])
                for item in [
                    part.split("=") for part in os.path.basename(dir_name).split("__")
                ]
            }
            if selector.matches(attributes):
                with open(dir_name + "/dataset.pickle", "rb") as fp:
                    dataset = pickle.load(fp)
                datasets.append(dataset)
        return DatasetCollection(datasets)

    def save(self, dataset: Dataset):
        full_path = (
            self.base_dir / dataset.identifier.key.replace("/", "__") / "dataset.pickle"
        )
        full_path.parent.mkdir(parents=True, exist_ok=True)

        with open(full_path, "wb") as fp:
            pickle.dump(dataset, fp)

    def next_identity(self):
        return str(uuid.uuid4())
