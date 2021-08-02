from domain.models import (
    FileRepository,
    Dataset,
    DatasetCollection,
    File, DraftFile,
    DatasetRepository,
    DatasetSelector,
    DatasetVersion
)


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
        pass

    def add_version(self, dataset: Dataset, version: DatasetVersion):
        """
        Convert draft files to regular files (same them to repository), and
        save new version to dataset.
        """
        files = {}

        for filename, file_ in version.files.items():
            if isinstance(file_, DraftFile):
                # TODO: check if this is a very clean way to go from DraftFile to File
                #
                # The format of the file_id is depending on the FileRepository type
                # For example S3FileRepository can use a full key as file_id,
                # while some database storage can use an uuid. It's up to the
                # repository to define the file_id
                file_id = self.file_repository.get_identify(
                    dataset, version, filename
                )
                file = File.from_draft(file_, file_id)

                self.file_repository.save_content(file_id, file_.stream)

                files[filename] = file
            else:
                files[filename] = file_

        final_version = DatasetVersion(
            created_at=version.created_at,
            description=version.description,
            files=files
        )
        dataset.add_version(final_version)
