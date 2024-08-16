import logging
import platform
from multiprocessing import set_start_method, cpu_count
from typing import List

from ingestify.domain.models import Dataset, Identifier, Selector, Source, Task, TaskSet
from ingestify.utils import map_in_pool, TaskExecutor

from .dataset_store import DatasetStore
from ..domain.models.data_spec_version_collection import DataSpecVersionCollection
from ..domain.models.extract_job import ExtractJob
from ..exceptions import ConfigurationError

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
        data_spec_versions: DataSpecVersionCollection,
        store: DatasetStore,
    ):
        self.source = source
        self.dataset = dataset
        self.dataset_identifier = dataset_identifier
        self.data_spec_versions = data_spec_versions
        self.store = store

    def run(self):
        files = self.source.fetch_dataset_files(
            self.dataset.dataset_type,
            self.dataset_identifier,  # Use the new dataset_identifier as it's more up-to-date, and contains more info
            data_spec_versions=self.data_spec_versions,
            current_revision=self.dataset.current_revision,
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
        data_spec_versions: DataSpecVersionCollection,
        dataset_identifier: Identifier,
        store: DatasetStore,
    ):
        self.source = source
        self.dataset_type = dataset_type
        self.data_spec_versions = data_spec_versions
        self.dataset_identifier = dataset_identifier
        self.store = store

    def run(self):
        files = self.source.fetch_dataset_files(
            dataset_type=self.dataset_type,
            identifier=self.dataset_identifier,
            data_spec_versions=self.data_spec_versions,
            current_revision=None,
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
        total_dataset_count = 0

        # First collect all selectors, before discovering datasets
        selectors = {}
        for extract_job in self.extract_jobs:
            static_selectors = [
                selector
                for selector in extract_job.selectors
                if not selector.is_dynamic
            ]
            dynamic_selectors = [
                selector for selector in extract_job.selectors if selector.is_dynamic
            ]
            if dynamic_selectors:
                if hasattr(extract_job.source, "discover_selectors"):
                    logger.debug(
                        f"Discovering selectors from {extract_job.source.__class__.__name__}"
                    )

                    # TODO: consider making this lazy and fetch once per Source instead of
                    #       once per ExtractJob
                    all_selectors = extract_job.source.discover_selectors(
                        extract_job.dataset_type
                    )
                    extra_static_selectors = []
                    for dynamic_selector in dynamic_selectors:
                        dynamic_job_selectors = [
                            Selector.build(
                                job_selector,
                                data_spec_versions=extract_job.data_spec_versions,
                            )
                            for job_selector in all_selectors
                            if dynamic_selector.is_match(job_selector)
                        ]
                        extra_static_selectors.extend(dynamic_job_selectors)
                        logger.info(f"Added {len(dynamic_job_selectors)} selectors")

                    static_selectors.extend(extra_static_selectors)
                    logger.info(
                        f"Discovered {len(extra_static_selectors)} selectors from {extract_job.source.__class__.__name__}"
                    )
                else:
                    raise ConfigurationError(
                        f"Dynamic selectors cannot be used for "
                        f"{extract_job.source.__class__.__name__} because it doesn't support"
                        f" selector discovery"
                    )

            # Merge selectors when source, dataset_type and actual selector is the same. This makes
            # sure there will be only 1 dataset for this combination
            for selector in static_selectors:
                key = (extract_job.source.name, extract_job.dataset_type, selector.key)
                if existing_selector := selectors.get(key):
                    existing_selector[1].data_spec_versions.merge(
                        selector.data_spec_versions
                    )
                else:
                    selectors[key] = (extract_job, selector)

        def run_task(task):
            logger.info(f"Running task {task}")
            task.run()

        task_executor = TaskExecutor()

        for extract_job, selector in selectors.values():
            logger.debug(
                f"Discovering datasets from {extract_job.source.__class__.__name__} using selector {selector}"
            )

            dataset_collection_metadata = self.store.get_dataset_collection(
                dataset_type=extract_job.dataset_type,
                data_spec_versions=selector.data_spec_versions,
                selector=selector,
                metadata_only=True,
            ).metadata

            # There are two different, but similar flows here:
            # 1. The discover_datasets returns a list, and the entire list can be processed at once
            # 2. The discover_datasets returns an iterator of batches, in this case we need to process each batch
            discovered_datasets = extract_job.source.discover_datasets(
                dataset_type=extract_job.dataset_type,
                data_spec_versions=selector.data_spec_versions,
                dataset_collection_metadata=dataset_collection_metadata,
                **selector.filtered_attributes,
            )

            if isinstance(discovered_datasets, list):
                batches = [discovered_datasets]
            else:
                batches = discovered_datasets

            for batch in batches:
                dataset_identifiers = [
                    Identifier.create_from(selector, **identifier)
                    # We have to pass the data_spec_versions here as a Source can add some
                    # extra data to the identifier which is retrieved in a certain data format
                    for identifier in batch
                ]

                # Load all available datasets based on the discovered dataset identifiers
                dataset_collection = self.store.get_dataset_collection(
                    dataset_type=extract_job.dataset_type,
                    provider=extract_job.source.provider,
                    selector=dataset_identifiers,
                )

                skip_count = 0
                total_dataset_count += len(dataset_identifiers)

                task_set = TaskSet()
                for dataset_identifier in dataset_identifiers:
                    if dataset := dataset_collection.get(dataset_identifier):
                        if extract_job.fetch_policy.should_refetch(
                            dataset, dataset_identifier
                        ):
                            task_set.add(
                                UpdateDatasetTask(
                                    source=extract_job.source,
                                    dataset=dataset,  # Current dataset from the database
                                    dataset_identifier=dataset_identifier,  # Most recent dataset_identifier
                                    data_spec_versions=selector.data_spec_versions,
                                    store=self.store,
                                )
                            )
                        else:
                            skip_count += 1
                    else:
                        if extract_job.fetch_policy.should_fetch(dataset_identifier):
                            task_set.add(
                                CreateDatasetTask(
                                    source=extract_job.source,
                                    dataset_type=extract_job.dataset_type,
                                    dataset_identifier=dataset_identifier,
                                    data_spec_versions=selector.data_spec_versions,
                                    store=self.store,
                                )
                            )
                        else:
                            skip_count += 1

                logger.info(
                    f"Discovered {len(dataset_identifiers)} datasets from {extract_job.source.__class__.__name__} "
                    f"using selector {selector} => {len(task_set)} tasks. {skip_count} skipped."
                )

                task_executor.run(run_task, task_set)
                logger.info(f"Scheduled {len(task_set)} tasks")

        task_executor.join()

        logger.info("Done")
