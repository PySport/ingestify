from domain.models import DatasetSelector, DatasetIdentifier, Dataset, DatasetVersion


class Store:
    def get_datasets(self, dataset_selector: dict) -> List[Dataset]:
        return None

    def add(self, dataset_identifier: DatasetIdentifier, dataset_version: DatasetVersion):
        pass
