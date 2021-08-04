from typing import Dict

from domain.models import (
    FileRepository,
    Dataset,
    DatasetCollection,
    File,
    DraftFile,
    DatasetRepository,
    DatasetSelector,
    DatasetVersion,
    DatasetIdentifier,
)
from utils import utcnow


class Store:
    def __init__(
        self,
        dataset_repository: DatasetRepository,
        file_repository: FileRepository,
    ):
        self.dataset_repository = dataset_repository
        self.file_repository = file_repository

    def get_dataset_collection(
        self, dataset_selector: DatasetSelector
    ) -> DatasetCollection:
        return self.dataset_repository.get_dataset_collection(dataset_selector)

    def _persist_version(
        self, dataset: Dataset, version: DatasetVersion
    ) -> DatasetVersion:
        files = {}

        for filename, file_ in version.files.items():
            if isinstance(file_, DraftFile):
                # TODO: check if this is a very clean way to go from DraftFile to File
                #
                # The format of the file_id is depending on the FileRepository type
                # For example S3FileRepository can use a full key as file_id,
                # while some database storage can use an uuid. It's up to the
                # repository to define the file_id
                file_id = self.file_repository.get_identify(dataset, version, filename)
                file = File.from_draft(file_, file_id)

                self.file_repository.save_content(file_id, file_.stream)

                files[filename] = file
            else:
                files[filename] = file_

        return DatasetVersion(
            version_id=version.version_id,
            created_at=version.created_at,
            description=version.description,
            files=files,
        )

    def add_version(
        self, dataset: Dataset, files: Dict[str, DraftFile], description: str = "Update"
    ):
        """
        Create new version first, so FileRepository can use
        version_id in the key.
        """
        version_id = dataset.next_version_id()

        # TODO: decide if following code should be part of Dataset
        tmp_version = DatasetVersion(
            version_id=version_id,
            created_at=utcnow(),
            description=description,
            files=files,
        )
        version = self._persist_version(dataset, tmp_version)
        dataset.add_version(version)

        self.dataset_repository.save(dataset)

    def create_dataset(
        self,
        dataset_identifier: DatasetIdentifier,
        files: Dict[str, DraftFile],
        description: str = "Update",
    ):
        dataset = Dataset(
            dataset_id=self.dataset_repository.next_identity(),
            identifier=dataset_identifier,
        )
        self.add_version(dataset, files, description)
