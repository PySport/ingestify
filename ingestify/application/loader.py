import logging
import platform
from multiprocessing import set_start_method, cpu_count
from typing import List

from ingestify.domain.models import Dataset, Identifier, Selector, Source, Task, TaskSet
from ingestify.utils import map_in_pool

from .dataset_store import DatasetStore
from ..domain.models.extract_job import ExtractJob

if platform.system() == "Darwin":
    set_start_method("fork", force=True)
else:
    set_start_method("spawn", force=True)


logger = logging.getLogger(__name__)


class UpdateDatasetTask(Task):
    def __init__(
        self,
        source: Source,
        dataset: Dataset,
        dataset_identifier: Identifier,
        store: DatasetStore,
    ):
        self.source = source
        self.dataset = dataset
        self.dataset_identifier = dataset_identifier
        self.store = store

    def run(self):
        files = self.source.fetch_dataset_files(
            self.dataset.dataset_type,
            self.dataset_identifier,  # Use the new dataset_identifier as it's more up-to-date, and contains more info
            current_version=self.dataset.current_version,
        )
        self.store.update_dataset(
            dataset=self.dataset,
            dataset_identifier=self.dataset_identifier,
            files=files,
        )

    def __repr__(self):
        return f"UpdateDatasetTask({self.source} -> {self.dataset.identifier})"


class CreateDatasetTask(Task):
    def __init__(
        self,
        source: Source,
        dataset_type: str,
        dataset_identifier: Identifier,
        store: DatasetStore,
    ):
        self.source = source
        self.dataset_type = dataset_type
        self.dataset_identifier = dataset_identifier
        self.store = store

    def run(self):
        files = self.source.fetch_dataset_files(
            self.dataset_type, self.dataset_identifier, current_version=None
        )
        self.store.create_dataset(
            dataset_type=self.dataset_type,
            provider=self.source.provider,
            dataset_identifier=self.dataset_identifier,
            files=files,
        )

    def __repr__(self):
        return f"CreateDatasetTask({self.source} -> {self.dataset_identifier})"


class Loader:
    def __init__(self, store: DatasetStore):
        self.store = store
        self.extract_jobs: List[ExtractJob] = []

    def add_extract_job(self, extract_job: ExtractJob):
        self.extract_jobs.append(extract_job)

    def collect_and_run(self):
        task_set = TaskSet()

        total_dataset_count = 0

        # First collect all selectors, before discovering datasets
        selectors = {}
        for extract_job in self.extract_jobs:
            job_selectors = extract_job.selectors
            if len(job_selectors) == 1 and not job_selectors[0]:
                if hasattr(extract_job.source, "discover_selectors"):
                    logger.debug(
                        f"Discovering selectors from {extract_job.source.__class__.__name__}"
                    )
                    job_selectors = [
                        Selector.build(
                            **selector,
                            data_formats=extract_job.data_formats
                        )
                        for selector in extract_job.source.discover_selectors(
                            extract_job.dataset_type
                        )
                    ]
                    logger.info(
                        f"Discovered {len(job_selectors)} selectors from {extract_job.source.__class__.__name__}"
                    )
                else:
                    logger.info(
                        f"Empty selector and source {extract_job.source.__class__.__name__} "
                        f"doesn't support discover_selectors."
                    )

            # Merge selectors when source, dataset_type and actual selector is the same. This makes
            # sure there will be only 1 dataset for this combination
            for selector in job_selectors:
                key = (extract_job.source.name, extract_job.dataset_type, selector.key)
                if existing_selector := selectors.get(key):
                    existing_selector[1].data_formats.merge(selector.data_formats)
                else:
                    selectors[key] = (extract_job, selector)

        for extract_job, selector in selectors.values():
            logger.debug(
                f"Discovering datasets from {extract_job.source.__class__.__name__} using selector {selector}"
            )
            dataset_identifiers = [
                Identifier.create_from(selector, **identifier)

                # We have to pass the data_formats here as a Source can add some
                # extra data to the identifier which is retrieved in a certain data format
                for identifier in extract_job.source.discover_datasets(
                    dataset_type=extract_job.dataset_type,
                    data_formats=selector.data_formats,
                    **selector.filtered_attributes,
                )
            ]

            task_subset = TaskSet()

            dataset_collection = self.store.get_dataset_collection(
                dataset_type=extract_job.dataset_type,
                provider=extract_job.source.provider,
                selector=selector,
            )

            skip_count = 0
            total_dataset_count += len(dataset_identifiers)

            for dataset_identifier in dataset_identifiers:
                if dataset := dataset_collection.get(dataset_identifier):
                    if extract_job.fetch_policy.should_refetch(
                        dataset, dataset_identifier
                    ):
                        task_subset.add(
                            UpdateDatasetTask(
                                source=extract_job.source,
                                dataset=dataset,  # Current dataset from the database
                                dataset_identifier=dataset_identifier,  # Most recent dataset_identifier
                                data_formats=selector.data_formats,
                                store=self.store,
                            )
                        )
                    else:
                        skip_count += 1
                else:
                    if extract_job.fetch_policy.should_fetch(dataset_identifier):
                        task_subset.add(
                            CreateDatasetTask(
                                source=extract_job.source,
                                dataset_type=extract_job.dataset_type,
                                dataset_identifier=dataset_identifier,
                                data_formats=selector.data_formats,
                                store=self.store,
                            )
                        )
                    else:
                        skip_count += 1

            logger.info(
                f"Discovered {len(dataset_identifiers)} datasets from {extract_job.source.__class__.__name__} "
                f"using selector {selector} => {len(task_subset)} tasks. {skip_count} skipped."
            )

            task_set += task_subset

        if len(task_set):
            processes = cpu_count()
            logger.info(f"Scheduled {len(task_set)} tasks. With {processes} processes")

            def run_task(task):
                logger.info(f"Running task {task}")
                task.run()

            map_in_pool(run_task, task_set)
        else:
            logger.info("Nothing to do.")
