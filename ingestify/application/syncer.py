import logging

from datetime import timedelta
from typing import Dict

from domain.models import Dataset, DatasetIdentifier, DatasetSelector, source_factory
from infra.store import LocalDatasetRepository, LocalFileRepository
from utils import utcnow

from .store import Store

logger = logging.getLogger(__name__)


class FetchPolicy:
    def __init__(self):
        # refresh all data that changed less than two day ago
        self.min_age = utcnow() - timedelta(days=2)
        self.last_change = utcnow() - timedelta(days=1)

    def should_fetch(self, dataset_identifier: DatasetIdentifier) -> bool:
        # this is called when dataset does not exist yet
        return True

    def should_refetch(self, dataset: Dataset) -> bool:
        current_version = dataset.current_version

        if not dataset.versions:
            return True
        elif self.last_change > current_version.created_at > self.min_age:
            return True
        else:
            return False


def sync_store(source_name: str, dataset_selector: Dict):
    source = source_factory.build(source_name)

    file_repository = LocalFileRepository("/tmp/blaat/files")
    dataset_repository = LocalDatasetRepository("/tmp/blaat/datasets")

    store = Store(
        dataset_repository=dataset_repository, file_repository=file_repository
    )

    fetch_policy = FetchPolicy()

    selector = DatasetSelector(**dataset_selector)

    logger.info(f"Discovering datasets")
    dataset_identifiers = source.discover_datasets(selector)
    logger.info(f"Found {len(dataset_identifiers)} datasets")
    dataset_collection = store.get_dataset_collection(selector)

    for dataset_identifier in dataset_identifiers:
        if dataset := dataset_collection.get(dataset_identifier):
            if fetch_policy.should_refetch(dataset):
                logger.debug(f"Going to update {dataset_identifier}")
                files = source.fetch_dataset_files(
                    dataset_identifier, current_version=dataset.current_version
                )
                store.add_version(dataset, files)
            else:
                logger.debug(f"Skipping to update {dataset_identifier}")
        else:
            if fetch_policy.should_fetch(dataset_identifier):
                logger.debug(f"Fetching {dataset_identifier}")
                files = source.fetch_dataset_files(
                    dataset_identifier, current_version=None
                )
                store.create_dataset(dataset_identifier, files)
