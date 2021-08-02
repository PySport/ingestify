from domain.models import (
    ContentRepository,
    Dataset,
    DatasetCollection,
    DatasetContent,
    DatasetRepository,
    DatasetSelector,
    DatasetVersion,
    DraftDatasetVersion,
)


class Store:
    def __init__(
        self,
        dataset_repository: DatasetRepository,
        content_repository: ContentRepository,
    ):
        self.dataset_repository = dataset_repository
        self.content_repository = content_repository

    def get_dataset_collection(
        self, dataset_selector: DatasetSelector
    ) -> DatasetCollection:
        pass

    def add_version(self, dataset: Dataset, version: DraftDatasetVersion):
        final_version = DatasetVersion(
            tag=version.tag,
            modified_at=version.modified_at,
            content_id=self.content_repository.next_identify(),
            content=DatasetContent(
                content_type=version.content_type,
            ),
        )
        dataset.add_version(final_version)
