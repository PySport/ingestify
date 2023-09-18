from datetime import timedelta

from ingestify.domain import Dataset, Identifier
from ingestify.utils import utcnow


class FetchPolicy:
    def __init__(self):
        # refresh all data that changed less than two day ago
        self.min_age = utcnow() - timedelta(days=2)
        self.last_change = utcnow() - timedelta(days=1)

    def should_fetch(self, dataset_identifier: Identifier) -> bool:
        # this is called when dataset does not exist yet
        return True

    def should_refetch(self, dataset: Dataset, identifier: Identifier) -> bool:
        current_version = dataset.current_version

        if not dataset.versions:
            # TODO: this is weird? Dataset without any data. Fetch error?
            return True
        elif current_version and identifier.last_modified and current_version.created_at < identifier.last_modified:
            return True
        # elif self.last_change > current_version.created_at > self.min_age:
        #     return True
        else:
            return False
