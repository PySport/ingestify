import logging
from datetime import timedelta
from multiprocessing import Pool, set_start_method, cpu_count
from typing import Dict, List, Tuple

from ingestify.domain.models import Dataset, Identifier, Selector, Source, Task, TaskSet
from ingestify.utils import utcnow

from .dataset_store import DatasetStore

logger = logging.getLogger(__name__)


class FetchPolicy:
    def __init__(self):
        # refresh all data that changed less than two day ago
        self.min_age = utcnow() - timedelta(days=2)
        self.last_change = utcnow() - timedelta(days=1)

    def should_fetch(self, dataset_identifier: Identifier) -> bool:
        # this is called when dataset does not exist yet
        return True

    def should_refetch(self, dataset: Dataset) -> bool:
        current_version = dataset.current_version

        if not dataset.versions:
            # TODO: this is weird? Dataset without any data. Fetch error?
            return True
        # elif self.last_change > current_version.created_at > self.min_age:
        #    return True
        else:
            return False


class UpdateDatasetTask(Task):
    def __init__(self, source: Source, dataset: Dataset, store: DatasetStore):
        self.source = source
        self.dataset = dataset
        self.store = store

    def run(self):
        files = self.source.fetch_dataset_files(
            self.dataset.identifier, current_version=self.dataset.current_version
        )
        self.store.add_version(self.dataset, files)

    def __repr__(self):
        return f"UpdateDatasetTask({self.source} -> {self.dataset.identifier})"


class CreateDatasetTask(Task):
    def __init__(
        self, source: Source, dataset_identifier: Identifier, store: DatasetStore
    ):
        self.source = source
        self.dataset_identifier = dataset_identifier
        self.store = store

    def run(self):
        files = self.source.fetch_dataset_files(
            self.dataset_identifier, current_version=None
        )
        self.store.create_dataset(
            dataset_type=self.source.dataset_type,
            provider=self.source.provider,
            dataset_identifier=self.dataset_identifier,
            files=files,
        )

    def __repr__(self):
        return f"CreateDatasetTask({self.source} -> {self.dataset_identifier})"


class Loader:
    def __init__(self, sources: Dict[str, Source], store: DatasetStore):
        self.store = store
        self.sources = sources

        self.selectors: List[Tuple[Source, Selector]] = []
        self.fetch_policy = FetchPolicy()

    def add_selector(self, source: str, selector: Dict):
        self.selectors.append((self.sources[source], Selector(**selector)))

    def collect_and_run(self):
        task_set = TaskSet()

        total_dataset_count = 0
        for source, selector in self.selectors:
            logger.debug(
                f"Discovering datasets from {source.__class__.__name__} using selector {selector}"
            )
            dataset_identifiers = [
                Identifier.create_from(selector, **identifier)
                for identifier in source.discover_datasets(
                    **selector.filtered_attributes
                )
            ]

            task_subset = TaskSet()

            dataset_collection = self.store.get_dataset_collection(
                dataset_type=source.dataset_type,
                provider=source.provider,
                selector=selector,
            )

            skip_count = 0
            total_dataset_count += len(dataset_identifiers)

            for dataset_identifier in dataset_identifiers:
                if dataset := dataset_collection.get(dataset_identifier):
                    if self.fetch_policy.should_refetch(dataset):
                        task_subset.add(
                            UpdateDatasetTask(
                                source=source, dataset=dataset, store=self.store
                            )
                        )
                    else:
                        skip_count += 1
                else:
                    if self.fetch_policy.should_fetch(dataset_identifier):
                        task_subset.add(
                            CreateDatasetTask(
                                source=source,
                                dataset_identifier=dataset_identifier,
                                store=self.store,
                            )
                        )
                    else:
                        skip_count += 1

            logger.info(
                f"Discovered {len(dataset_identifiers)} datasets from {source.__class__.__name__} using selector {selector} => {len(task_subset)} tasks. {skip_count} skipped."
            )

            task_set += task_subset

        if len(task_set):
            processes = cpu_count()
            logger.info(f"Scheduled {len(task_set)} tasks. With {processes} processes")

            def run_task(task):
                logger.info(f"Running task {task}")
                task.run()

            set_start_method("fork")
            with Pool(processes) as pool:
                pool.map(run_task, task_set)
        else:
            logger.info("Nothing to do.")
