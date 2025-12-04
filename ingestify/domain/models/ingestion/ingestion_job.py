import itertools
import json
import logging
import uuid
from enum import Enum
from typing import Optional, Iterator, Union

from pydantic import ValidationError

from ingestify import retrieve_http
from ingestify.application.dataset_store import DatasetStore
from ingestify.domain import Selector, Identifier, TaskSet, Dataset, DraftFile, Task
from ingestify.domain.models.dataset.file import NotModifiedFile
from ingestify.domain.models.dataset.revision import RevisionSource, SourceType
from ingestify.domain.models.ingestion.ingestion_job_summary import (
    IngestionJobSummary,
)
from ingestify.domain.models.ingestion.ingestion_plan import IngestionPlan
from ingestify.domain.models.dataset.events import SelectorSkipped, DatasetSkipped
from ingestify.domain.models.resources.dataset_resource import (
    FileResource,
    DatasetResource,
)
from ingestify.domain.models.task.task_summary import TaskSummary
from ingestify.exceptions import SaveError, IngestifyError
from ingestify.utils import TaskExecutor, chunker

logger = logging.getLogger(__name__)


DEFAULT_CHUNK_SIZE = 1000


def run_task(task):
    logger.info(f"Running task {task}")
    return task.run()


def to_batches(input_):
    if isinstance(input_, list):
        batches = iter(input_)
    else:
        # Assume it's an iterator. Peek what's inside, and put it back
        try:
            peek = next(input_)
        except StopIteration:
            # Nothing to batch
            return iter([])

        input_ = itertools.chain([peek], input_)

        if not isinstance(peek, list):
            batches = chunker(input_, DEFAULT_CHUNK_SIZE)
        else:
            batches = input_
    return batches


def load_file(
    file_resource: FileResource, dataset: Optional[Dataset] = None
) -> Union[DraftFile, NotModifiedFile]:
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
            return NotModifiedFile(
                modified_at=file_resource.last_modified,
                reason="tag matched current_file",
            )
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
            files = {
                file_id: task_summary.record_load_file(
                    lambda: load_file(file_resource, dataset=self.dataset),
                    metadata={"file_id": file_id},
                )
                for file_id, file_resource in self.dataset_resource.files.items()
            }

            self.dataset_resource.run_post_load_files(files)

            try:
                revision = self.store.update_dataset(
                    dataset=self.dataset,
                    name=self.dataset_resource.name,
                    state=self.dataset_resource.state,
                    metadata=self.dataset_resource.metadata,
                    files=files,
                    revision_source=revision_source,
                )
                task_summary.set_stats_from_revision(revision)
            except Exception as e:
                raise SaveError("Could not update dataset") from e

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
            files = {
                file_id: task_summary.record_load_file(
                    lambda: load_file(file_resource, dataset=None),
                    metadata={"file_id": file_id},
                )
                for file_id, file_resource in self.dataset_resource.files.items()
            }

            self.dataset_resource.run_post_load_files(files)

            try:
                revision = self.store.create_dataset(
                    dataset_type=self.dataset_resource.dataset_type,
                    provider=self.dataset_resource.provider,
                    dataset_identifier=dataset_identifier,
                    name=self.dataset_resource.name,
                    state=self.dataset_resource.state,
                    metadata=self.dataset_resource.metadata,
                    files=files,
                    revision_source=revision_source,
                )

                task_summary.set_stats_from_revision(revision)
            except Exception as e:
                raise SaveError("Could not create dataset") from e

        return task_summary

    def __repr__(self):
        return f"CreateDatasetTask({self.dataset_resource.provider} -> {self.dataset_resource.dataset_resource_id})"


MAX_TASKS_PER_CHUNK = 10_000


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
    ) -> Iterator[IngestionJobSummary]:
        is_first_chunk = True
        ingestion_job_summary = IngestionJobSummary.new(ingestion_job=self)
        # Process all items in batches. Yield a IngestionJobSummary per batch

        logger.info("Finding metadata")
        with ingestion_job_summary.record_timing("get_dataset_collection_metadata"):
            dataset_collection_metadata = store.get_dataset_collection(
                dataset_type=self.ingestion_plan.dataset_type,
                provider=self.ingestion_plan.source.provider,
                data_spec_versions=self.selector.data_spec_versions,
                selector=self.selector,
                metadata_only=True,
            ).metadata
        logger.info(f"Done: {dataset_collection_metadata}")

        if self.selector.last_modified and dataset_collection_metadata.last_modified:
            # This check might fail when the data_spec_versions is changed;
            # missing files are not detected
            if self.selector.last_modified < dataset_collection_metadata.last_modified:
                logger.info(
                    f"Skipping find_datasets because selector last_modified "
                    f"'{self.selector.last_modified}' < metadata last_modified "
                    f"'{dataset_collection_metadata.last_modified}'"
                )
                # Emit event for streaming datasets
                store.dispatch(SelectorSkipped(selector=self.selector))

                ingestion_job_summary.set_skipped()
                yield ingestion_job_summary
                return

        # There are two different, but similar flows here:
        # 1. The discover_datasets returns a list, and the entire list can be processed at once
        # 2. The discover_datasets returns an iterator of batches, in this case we need to process each batch
        try:
            logger.info(f"Finding datasets for selector={self.selector}")
            with ingestion_job_summary.record_timing("find_datasets"):
                dataset_resources = self.ingestion_plan.source.find_datasets(
                    dataset_type=self.ingestion_plan.dataset_type,
                    data_spec_versions=self.selector.data_spec_versions,
                    dataset_collection_metadata=dataset_collection_metadata,
                    **self.selector.custom_attributes,
                )

                # We need to include the to_batches as that will start the generator
                batches = to_batches(dataset_resources)
        except ValidationError as e:
            # Make sure to pass this to the highest level as this means the Source is wrong
            if "Field required" in str(e):
                raise IngestifyError("failed to run find_datasets") from e
            else:
                logger.exception("Failed to find datasets")

                ingestion_job_summary.set_exception(e)
                yield ingestion_job_summary
                return
        except Exception as e:
            logger.exception("Failed to find datasets")

            ingestion_job_summary.set_exception(e)
            yield ingestion_job_summary
            return

        logger.info("Starting tasks")

        while True:
            logger.info(f"Finding next batch of datasets for selector={self.selector}")

            try:
                with ingestion_job_summary.record_timing("find_datasets"):
                    try:
                        batch = next(batches)
                    except StopIteration:
                        break
            except Exception as e:
                logger.exception("Failed to fetch next batch")

                ingestion_job_summary.set_exception(e)
                yield ingestion_job_summary
                return

            dataset_identifiers = [
                Identifier.create_from_selector(
                    self.selector, **dataset_resource.dataset_resource_id
                )
                # We have to pass the data_spec_versions here as a Source can add some
                # extra data to the identifier which is retrieved in a certain data format
                for dataset_resource in batch
            ]

            logger.info(f"Searching for existing Datasets for DatasetResources")

            with ingestion_job_summary.record_timing("get_dataset_collection"):
                # Load all available datasets based on the discovered dataset identifiers
                dataset_collection = store.get_dataset_collection(
                    dataset_type=self.ingestion_plan.dataset_type,
                    # Assume all DatasetResources share the same provider
                    provider=batch[0].provider,
                    selector=dataset_identifiers,
                )

            skipped_tasks = 0

            task_set = TaskSet()

            with ingestion_job_summary.record_timing("build_task_set"):
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
                            # Emit event for streaming datasets
                            store.dispatch(DatasetSkipped(dataset=dataset))
                            skipped_tasks += 1
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
                            skipped_tasks += 1

            with ingestion_job_summary.record_timing("tasks"):
                if task_set:
                    logger.info(
                        f"Discovered {len(dataset_identifiers)} datasets from {self.ingestion_plan.source.__class__.__name__} "
                        f"using selector {self.selector} => {len(task_set)} tasks. {skipped_tasks} skipped."
                    )
                    logger.info(f"Running {len(task_set)} tasks")

                    task_summaries = task_executor.run(run_task, task_set)

                    ingestion_job_summary.add_task_summaries(task_summaries)
                else:
                    logger.info(
                        f"Discovered {len(dataset_identifiers)} datasets from {self.ingestion_plan.source.__class__.__name__} "
                        f"using selector {self.selector} => nothing to do"
                    )
                ingestion_job_summary.increase_skipped_tasks(skipped_tasks)

            if ingestion_job_summary.task_count() >= MAX_TASKS_PER_CHUNK:
                ingestion_job_summary.set_finished()
                yield ingestion_job_summary

                # Start a new one
                is_first_chunk = False
                ingestion_job_summary = IngestionJobSummary.new(ingestion_job=self)

        if ingestion_job_summary.task_count() > 0 or is_first_chunk:
            # When there is interesting information to store, or there was no data at all, store it
            ingestion_job_summary.set_finished()
            yield ingestion_job_summary
