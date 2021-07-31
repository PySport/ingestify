from domain.models import Dataset, DatasetCollection, DatasetSelector, DraftDatasetVersion


class Store:
    def __init__(self, dataset_repository: DatasetRepository, content_repository: ContentRepository):
    def get_dataset_collection(
        self, dataset_selector: DatasetSelector
    ) -> DatasetCollection:
        pass

    def add_version(self, dataset: Dataset, version: DraftDatasetVersion):
        dataset.add_version(final_version)
