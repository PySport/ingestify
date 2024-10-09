import itertools
import json
import logging
import platform
from multiprocessing import set_start_method, cpu_count
from typing import List, Optional

from ingestify.domain.models import Dataset, Identifier, Selector, Source, Task, TaskSet
from ingestify.utils import map_in_pool, TaskExecutor, chunker

from .dataset_store import DatasetStore
from .. import DatasetResource, retrieve_http
from ..domain import DraftFile
from ..domain.models.data_spec_version_collection import DataSpecVersionCollection
from ..domain.models.extract_job import ExtractJob
from ..domain.models.resources.dataset_resource import FileResource
from ..exceptions import ConfigurationError

if platform.system() == "Darwin":
    set_start_method("fork", force=True)
else:
    set_start_method("spawn", force=True)


logger = logging.getLogger(__name__)


DEFAULT_CHUNK_SIZE = 1000


def to_batches(input_):
    if isinstance(input_, list):
        batches = [input_]
    else:
        # Assume it's an iterator. Peek what's inside, and put it back
        try:
            peek = next(input_)
        except StopIteration:
            # Nothing to batch
            return []

        input_ = itertools.chain([peek], input_)

        if not isinstance(peek, list):
            batches = chunker(input_, DEFAULT_CHUNK_SIZE)
        else:
            batches = input_
    return batches


def load_file(
    file_resource: FileResource, dataset: Optional[Dataset] = None
) -> Optional[DraftFile]:
    current_file = None
    if dataset:
        current_file = dataset.current_revision.modified_files_map.get(
            file_resource.file_id
        )

    if file_resource.json_content is not None:
        # Empty dictionary is allowed
        file = DraftFile.from_input(
            file_=json.dumps(file_resource.json_content, indent=4),
            data_serialization_format="json",
            data_feed_key=file_resource.data_feed_key,
            data_spec_version=file_resource.data_spec_version,
            modified_at=file_resource.last_modified,
        )
        if current_file and current_file.tag == file.tag:
            # Nothing changed
            return None
        return file
    elif file_resource.url:
        http_options = {}
        if file_resource.http_options:
            for k, v in file_resource.http_options.items():
                http_options[f"http_{k}"] = v

        return retrieve_http(
            url=file_resource.url,
            current_file=current_file,
            file_data_feed_key=file_resource.data_feed_key,
            file_data_spec_version=file_resource.data_spec_version,
            file_data_serialization_format=file_resource.data_serialization_format
            or "txt",
            **http_options,
        )
    else:
        return file_resource.file_loader(file_resource, current_file)


class UpdateDatasetTask(Task):
    def __init__(
        self,
        dataset: Dataset,
        dataset_resource: DatasetResource,
        store: DatasetStore,
    ):
        self.dataset = dataset
        self.dataset_resource = dataset_resource
        self.store = store

    def run(self):
        self.store.update_dataset(
            dataset=self.dataset,
            dataset_resource=self.dataset_resource,
            files={
                file_id: load_file(file_resource, dataset=self.dataset)
                for file_id, file_resource in self.dataset_resource.files.items()
            },
        )

    def __repr__(self):
        return f"UpdateDatasetTask({self.dataset_resource.provider} -> {self.dataset_resource.dataset_resource_id})"


class CreateDatasetTask(Task):
    def __init__(
        self,
        dataset_resource: DatasetResource,
        store: DatasetStore,
    ):
        self.dataset_resource = dataset_resource
        self.store = store

    def run(self):
        self.store.create_dataset(
            dataset_type=self.dataset_resource.dataset_type,
            provider=self.dataset_resource.provider,
            dataset_identifier=Identifier(**self.dataset_resource.dataset_resource_id),
            name=self.dataset_resource.name,
            state=self.dataset_resource.state,
            metadata=self.dataset_resource.metadata,
            files={
                file_id: load_file(file_resource)
                for file_id, file_resource in self.dataset_resource.files.items()
            },
        )

    def __repr__(self):
        return f"CreateDatasetTask({self.dataset_resource.provider} -> {self.dataset_resource.dataset_resource_id})"


class Loader:
    def __init__(self, store: DatasetStore):
        self.store = store
        self.extract_jobs: List[ExtractJob] = []

    def add_extract_job(self, extract_job: ExtractJob):
        self.extract_jobs.append(extract_job)

    def collect_and_run(self, dry_run: bool = False, provider: Optional[str] = None):
        total_dataset_count = 0

        # First collect all selectors, before discovering datasets
        selectors = {}
        for extract_job in self.extract_jobs:
            if provider is not None:
                if extract_job.source.provider != provider:
                    logger.info(
                        f"Skipping {extract_job } because provider doesn't match '{provider}'"
                    )
                    continue

            static_selectors = [
                selector
                for selector in extract_job.selectors
                if not selector.is_dynamic
            ]
            dynamic_selectors = [
                selector for selector in extract_job.selectors if selector.is_dynamic
            ]

            no_selectors = len(static_selectors) == 1 and not bool(static_selectors[0])
            if dynamic_selectors or no_selectors:
                if hasattr(extract_job.source, "discover_selectors"):
                    logger.debug(
                        f"Discovering selectors from {extract_job.source.__class__.__name__}"
                    )

                    # TODO: consider making this lazy and fetch once per Source instead of
                    #       once per ExtractJob
                    all_selectors = extract_job.source.discover_selectors(
                        extract_job.dataset_type
                    )
                    if no_selectors:
                        # When there were no selectors specified, just use all of them
                        extra_static_selectors = [
                            Selector.build(
                                job_selector,
                                data_spec_versions=extract_job.data_spec_versions,
                            )
                            for job_selector in all_selectors
                        ]
                        static_selectors = []
                    else:
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
                    if not no_selectors:
                        # When there are no selectors and no discover_selectors, just pass it through. It might break
                        # later on
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
            datasets = extract_job.source.find_datasets(
                dataset_type=extract_job.dataset_type,
                data_spec_versions=selector.data_spec_versions,
                dataset_collection_metadata=dataset_collection_metadata,
                **selector.custom_attributes,
            )

            batches = to_batches(datasets)

            for batch in batches:
                dataset_identifiers = [
                    Identifier.create_from_selector(
                        selector, **dataset_resource.dataset_resource_id
                    )
                    # We have to pass the data_spec_versions here as a Source can add some
                    # extra data to the identifier which is retrieved in a certain data format
                    for dataset_resource in batch
                ]

                # Load all available datasets based on the discovered dataset identifiers
                dataset_collection = self.store.get_dataset_collection(
                    dataset_type=extract_job.dataset_type,
                    # Assume all DatasetResources share the same provider
                    provider=batch[0].provider,
                    selector=dataset_identifiers,
                )

                skip_count = 0
                total_dataset_count += len(dataset_identifiers)

                task_set = TaskSet()
                for dataset_resource in batch:
                    dataset_identifier = Identifier.create_from_selector(
                        selector, **dataset_resource.dataset_resource_id
                    )

                    if dataset := dataset_collection.get(dataset_identifier):
                        if extract_job.fetch_policy.should_refetch(
                            dataset, dataset_resource
                        ):
                            task_set.add(
                                UpdateDatasetTask(
                                    dataset=dataset,  # Current dataset from the database
                                    dataset_resource=dataset_resource,  # Most recent dataset_resource
                                    store=self.store,
                                )
                            )
                        else:
                            skip_count += 1
                    else:
                        if extract_job.fetch_policy.should_fetch(dataset_resource):
                            task_set.add(
                                CreateDatasetTask(
                                    dataset_resource=dataset_resource,
                                    store=self.store,
                                )
                            )
                        else:
                            skip_count += 1

                if task_set:
                    logger.info(
                        f"Discovered {len(dataset_identifiers)} datasets from {extract_job.source.__class__.__name__} "
                        f"using selector {selector} => {len(task_set)} tasks. {skip_count} skipped."
                    )
                    logger.info(f"Running {len(task_set)} tasks")
                    with TaskExecutor(dry_run=dry_run) as task_executor:
                        task_executor.run(run_task, task_set)
                else:
                    logger.info(
                        f"Discovered {len(dataset_identifiers)} datasets from {extract_job.source.__class__.__name__} "
                        f"using selector {selector} => nothing to do"
                    )

        logger.info("Done")
