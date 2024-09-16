from datetime import timedelta

from ingestify.domain import Dataset, Identifier, DatasetResource
from ingestify.utils import utcnow


class FetchPolicy:
    def __init__(self):
        # refresh all data that changed less than two day ago
        self.min_age = utcnow() - timedelta(days=2)
        self.last_change = utcnow() - timedelta(days=1)

    def should_fetch(self, dataset_resource: DatasetResource) -> bool:
        # this is called when dataset does not exist yet
        return True

    def should_refetch(
        self, dataset: Dataset, dataset_resource: DatasetResource
    ) -> bool:
        current_revision = dataset.current_revision
        if not dataset.revisions:
            # TODO: this is weird? Dataset without any data. Fetch error?
            return True
        elif current_revision:
            files_last_modified = {
                file.file_id: file.last_modified
                for file in dataset_resource.files.values()
            }
            if current_revision.is_changed(files_last_modified):
                return True

            # We don't set last_modified on Dataset level anymore, only on file level
            # else:
            #     if (
            #         identifier.last_modified
            #         and current_revision.created_at < identifier.last_modified
            #     ):
            #         return True

        return False
