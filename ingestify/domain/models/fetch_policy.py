from datetime import timedelta

from ingestify.domain import Dataset, Identifier, DatasetResource
from ingestify.domain.models.dataset.dataset import DatasetSummary
from ingestify.domain.models.dataset.revision import RevisionState
from ingestify.utils import utcnow


class FetchPolicy:
    def __init__(self):
        # refresh all data that changed less than two day ago
        self.min_age = utcnow() - timedelta(days=2)
        self.last_change = utcnow() - timedelta(days=1)

    def should_fetch(self, dataset_resource: DatasetResource) -> bool:
        # this is called when dataset does not exist yet
        return True

    def can_skip(
        self, summary: DatasetSummary, dataset_resource: DatasetResource
    ) -> bool:
        """Cheap, one-sided pre-check (bloom-filter style) for an *existing*
        dataset. Return True only when certain, from the lightweight ``summary``,
        that the dataset is up-to-date: the engine then skips it without loading
        the full Dataset or reaching ``should_refetch``. Returning False means
        "unknown" — fall through to the authoritative path.

        Base policy: skip when the stored dataset is at least as new as every file
        the source reports. (This is the timestamp pre-check the engine used to do
        inline; it now lives here as the single source of truth.)
        """
        if summary.last_modified is None or not dataset_resource.files:
            return False
        max_file_modified = max(
            f.last_modified for f in dataset_resource.files.values()
        )
        return summary.last_modified >= max_file_modified

    def should_refetch(
        self, dataset: Dataset, dataset_resource: DatasetResource
    ) -> bool:
        current_revision = dataset.current_revision
        if not dataset.revisions:
            # TODO: this is weird? Dataset without any data. Fetch error?
            return True
        elif current_revision:
            if current_revision.state == RevisionState.VALIDATION_FAILED:
                return True
            files_last_modified = {
                file.file_id: file.last_modified
                for file in dataset_resource.files.values()
            }
            if current_revision.is_changed(
                files_last_modified, dataset.last_modified_at
            ):
                return True

            # We don't set last_modified on Dataset level anymore, only on file level
            # else:
            #     if (
            #         identifier.last_modified
            #         and current_revision.created_at < identifier.last_modified
            #     ):
            #         return True

        return False
