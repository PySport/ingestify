import itertools
import json
import logging
import uuid
from typing import Optional

from ingestify import retrieve_http
from ingestify.application.dataset_store import DatasetStore
from ingestify.domain import Selector, Identifier, TaskSet, Dataset, DraftFile, Task
from ingestify.domain.models.dataset.revision import RevisionSource, SourceType
from ingestify.domain.models.ingestion.ingestion_job_summary import (
    IngestionJobSummary,
)
from ingestify.domain.models.ingestion.ingestion_plan import IngestionPlan
from ingestify.domain.models.resources.dataset_resource import (
    FileResource,
    DatasetResource,
)
from ingestify.domain.models.task.task_summary import TaskSummary
from ingestify.utils import TaskExecutor, chunker

logger = logging.getLogger(__name__)


DEFAULT_CHUNK_SIZE = 1000


def run_task(task):
    logger.info(f"Running task {task}")
    return task.run()


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
            last_modified=file_resource.last_modified,
            **http_options,
            **file_resource.loader_kwargs,
        )
    else:
        return file_resource.file_loader(
            file_resource,
            current_file,
            # TODO: check how to fix this with typehints
            **file_resource.loader_kwargs,
        )


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
        self.task_id = str(uuid.uuid1())

    def run(self):
        dataset_identifier = Identifier(**self.dataset_resource.dataset_resource_id)

        revision_source = RevisionSource(
            source_id=self.task_id, source_type=SourceType.TASK
        )

        with TaskSummary.update(
            self.task_id, dataset_identifier=dataset_identifier
        ) as task_summary:
            revision = self.store.update_dataset(
                dataset=self.dataset,
                name=self.dataset_resource.name,
                state=self.dataset_resource.state,
                metadata=self.dataset_resource.metadata,
                files={
                    file_id: task_summary.record_load_file(
                        lambda: load_file(file_resource, dataset=self.dataset),
                        metadata={"file_id": file_id},
                    )
                    for file_id, file_resource in self.dataset_resource.files.items()
                },
                revision_source=revision_source,
            )
            task_summary.set_stats_from_revision(revision)

        return task_summary

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
        self.task_id = str(uuid.uuid1())

    def run(self):
        dataset_identifier = Identifier(**self.dataset_resource.dataset_resource_id)
        revision_source = RevisionSource(
            source_id=self.task_id, source_type=SourceType.TASK
        )

        with TaskSummary.create(self.task_id, dataset_identifier) as task_summary:
            revision = self.store.create_dataset(
                dataset_type=self.dataset_resource.dataset_type,
                provider=self.dataset_resource.provider,
                dataset_identifier=dataset_identifier,
                name=self.dataset_resource.name,
                state=self.dataset_resource.state,
                metadata=self.dataset_resource.metadata,
                files={
                    file_id: task_summary.record_load_file(
                        lambda: load_file(file_resource, dataset=None),
                        metadata={"file_id": file_id},
                    )
                    for file_id, file_resource in self.dataset_resource.files.items()
                },
                revision_source=revision_source,
            )

            task_summary.set_stats_from_revision(revision)

        return task_summary

    def __repr__(self):
        return f"CreateDatasetTask({self.dataset_resource.provider} -> {self.dataset_resource.dataset_resource_id})"


class IngestionJob:
    def __init__(
        self,
        ingestion_job_id: str,
        ingestion_plan: IngestionPlan,
        selector: Selector,
    ):
        self.ingestion_job_id = ingestion_job_id
        self.ingestion_plan = ingestion_plan
        self.selector = selector

    def execute(
        self, store: DatasetStore, task_executor: TaskExecutor
    ) -> IngestionJobSummary:
        with IngestionJobSummary.new(ingestion_job=self) as ingestion_job_summary:
            with ingestion_job_summary.record_timing("get_dataset_collection"):
                dataset_collection_metadata = store.get_dataset_collection(
                    dataset_type=self.ingestion_plan.dataset_type,
                    data_spec_versions=self.selector.data_spec_versions,
                    selector=self.selector,
                    metadata_only=True,
                ).metadata

            # There are two different, but similar flows here:
            # 1. The discover_datasets returns a list, and the entire list can be processed at once
            # 2. The discover_datasets returns an iterator of batches, in this case we need to process each batch
            with ingestion_job_summary.record_timing("find_datasets"):
                # Timing might be incorrect as it is an iterator
                datasets = self.ingestion_plan.source.find_datasets(
                    dataset_type=self.ingestion_plan.dataset_type,
                    data_spec_versions=self.selector.data_spec_versions,
                    dataset_collection_metadata=dataset_collection_metadata,
                    **self.selector.custom_attributes,
                )

            batches = to_batches(datasets)

            with ingestion_job_summary.record_timing("tasks"):
                for batch in batches:
                    dataset_identifiers = [
                        Identifier.create_from_selector(
                            self.selector, **dataset_resource.dataset_resource_id
                        )
                        # We have to pass the data_spec_versions here as a Source can add some
                        # extra data to the identifier which is retrieved in a certain data format
                        for dataset_resource in batch
                    ]

                    # Load all available datasets based on the discovered dataset identifiers
                    dataset_collection = store.get_dataset_collection(
                        dataset_type=self.ingestion_plan.dataset_type,
                        # Assume all DatasetResources share the same provider
                        provider=batch[0].provider,
                        selector=dataset_identifiers,
                    )

                    skip_count = 0

                    task_set = TaskSet()
                    for dataset_resource in batch:
                        dataset_identifier = Identifier.create_from_selector(
                            self.selector, **dataset_resource.dataset_resource_id
                        )

                        if dataset := dataset_collection.get(dataset_identifier):
                            if self.ingestion_plan.fetch_policy.should_refetch(
                                dataset, dataset_resource
                            ):
                                task_set.add(
                                    UpdateDatasetTask(
                                        dataset=dataset,  # Current dataset from the database
                                        dataset_resource=dataset_resource,  # Most recent dataset_resource
                                        store=store,
                                    )
                                )
                            else:
                                skip_count += 1
                        else:
                            if self.ingestion_plan.fetch_policy.should_fetch(
                                dataset_resource
                            ):
                                task_set.add(
                                    CreateDatasetTask(
                                        dataset_resource=dataset_resource,
                                        store=store,
                                    )
                                )
                            else:
                                skip_count += 1

                    if task_set:
                        logger.info(
                            f"Discovered {len(dataset_identifiers)} datasets from {self.ingestion_plan.source.__class__.__name__} "
                            f"using selector {self.selector} => {len(task_set)} tasks. {skip_count} skipped."
                        )
                        logger.info(f"Running {len(task_set)} tasks")
                        ingestion_job_summary.add_task_summaries(
                            task_executor.run(run_task, task_set)
                        )
                    else:
                        logger.info(
                            f"Discovered {len(dataset_identifiers)} datasets from {self.ingestion_plan.source.__class__.__name__} "
                            f"using selector {self.selector} => nothing to do"
                        )

        return ingestion_job_summary
