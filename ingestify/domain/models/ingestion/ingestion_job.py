import inspect
import itertools
import json
import logging
import uuid
from enum import Enum
from functools import lru_cache
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
from ingestify.domain.models.dataset.dataset import DatasetLastModifiedAtMap
from ingestify.domain.models.task.task_summary import TaskSummary, Operation
from ingestify.exceptions import SaveError, IngestifyError, StopProcessing
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
    file_resource: FileResource,
    dataset: Optional[Dataset] = None,
    dataset_resource: Optional[DatasetResource] = None,
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
        extra_kwargs = {}
        if _loader_accepts_dataset_resource(file_resource.file_loader):
            extra_kwargs["dataset_resource"] = dataset_resource
        return file_resource.file_loader(
            file_resource,
            current_file,
            **extra_kwargs,
            **file_resource.loader_kwargs,
        )


@lru_cache(maxsize=None)
def _loader_accepts_dataset_resource(loader) -> bool:
    """Return True if loader accepts a `dataset_resource` keyword argument."""
    try:
        sig = inspect.signature(loader)
    except (TypeError, ValueError):
        return False
    params = sig.parameters
    if "dataset_resource" in params:
        return True
    return any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values())


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
                    lambda: load_file(
                        file_resource,
                        dataset=self.dataset,
                        dataset_resource=self.dataset_resource,
                    ),
                    metadata={"file_id": file_id},
                )
                for file_id, file_resource in self.dataset_resource.files.items()
            }

            self.dataset_resource.run_post_load_files(files, self.dataset)

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
                    lambda: load_file(
                        file_resource,
                        dataset=None,
                        dataset_resource=self.dataset_resource,
                    ),
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

# How many additional tasks must accumulate before a live progress snapshot of
# the IngestionJobSummary is persisted again. Keeps mid-run writes to roughly
# one per PROGRESS_SAVE_INTERVAL tasks instead of one per (possibly tiny) batch.
PROGRESS_SAVE_INTERVAL = 100


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
        # task_count() at which the summary was last persisted mid-run.
        self._last_progress_saved_at = 0

    def _save_progress(
        self,
        store: DatasetStore,
        ingestion_job_summary: IngestionJobSummary,
        *,
        force: bool = False,
    ):
        """Persist a live snapshot of the (still RUNNING) summary.

        Only the parent row plus any FAILED task summaries are written (see
        DatasetStore.save_ingestion_job_summary), so it is cheap enough to call
        repeatedly. Throttled to roughly every PROGRESS_SAVE_INTERVAL tasks
        unless ``force`` is set (used for the initial RUNNING row and when a new
        chunk starts). Works for both the sync find_datasets flow and the async
        submit/collect flow.
        """
        count = ingestion_job_summary.task_count()
        if not force and count - self._last_progress_saved_at < PROGRESS_SAVE_INTERVAL:
            return
        self._last_progress_saved_at = count
        ingestion_job_summary.recount()
        store.save_ingestion_job_summary(ingestion_job_summary)

    def execute(
        self,
        store: DatasetStore,
        task_executor: TaskExecutor,
        last_modified_at_map: Optional[DatasetLastModifiedAtMap] = None,
    ) -> Iterator[IngestionJobSummary]:
        is_first_chunk = True
        ingestion_job_summary = IngestionJobSummary.new(ingestion_job=self)
        # Persist the RUNNING row up front so the job is observable from the
        # moment it starts — and so a record survives even if the run never
        # reaches its final yield (e.g. an async source that keeps polling).
        self._last_progress_saved_at = 0
        self._save_progress(store, ingestion_job_summary, force=True)
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

        source = self.ingestion_plan.source
        if hasattr(source, "submit") and hasattr(source, "collect"):
            yield from self._execute_async(
                source,
                batches,
                store,
                task_executor,
                last_modified_at_map,
                ingestion_job_summary,
                is_first_chunk,
            )
            return

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

            # Fast pre-check: skip datasets that are definitely up-to-date
            # based on the cached timestamps. Only resources that might need
            # work proceed to the full get_dataset_collection check.
            skipped_tasks = 0
            if last_modified_at_map:
                pending_batch = []
                for dataset_resource in batch:
                    identifier = Identifier.create_from_selector(
                        self.selector, **dataset_resource.dataset_resource_id
                    )
                    ts = last_modified_at_map.get(identifier.key)
                    if ts is not None:
                        # Dataset exists — check if all files are up-to-date
                        max_file_modified = max(
                            f.last_modified for f in dataset_resource.files.values()
                        )
                        if ts >= max_file_modified:
                            skipped_tasks += 1
                            continue
                    pending_batch.append(dataset_resource)
                batch = pending_batch

            if not batch:
                logger.info(
                    f"Discovered {skipped_tasks + len(batch)} datasets from "
                    f"{self.ingestion_plan.source.__class__.__name__} "
                    f"using selector {self.selector} => nothing to do "
                    f"({skipped_tasks} skipped via pre-check)"
                )
                ingestion_job_summary.increase_skipped_tasks(skipped_tasks)
                continue

            dataset_identifiers = [
                Identifier.create_from_selector(
                    self.selector, **dataset_resource.dataset_resource_id
                )
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

                    try:
                        results = task_executor.run(run_task, task_set)
                    except StopProcessing:
                        logger.info(
                            "StopProcessing raised — saving partial results "
                            "and stopping"
                        )
                        ingestion_job_summary.set_finished()
                        yield ingestion_job_summary
                        raise

                    ingestion_job_summary.add_task_summaries(results)
                else:
                    logger.info(
                        f"Discovered {len(dataset_identifiers)} datasets from {self.ingestion_plan.source.__class__.__name__} "
                        f"using selector {self.selector} => nothing to do"
                    )
                ingestion_job_summary.increase_skipped_tasks(skipped_tasks)

            # Live snapshot after each batch (throttled) so the summary's
            # counters/state stay current while the job runs.
            self._save_progress(store, ingestion_job_summary)

            if ingestion_job_summary.task_count() >= MAX_TASKS_PER_CHUNK:
                ingestion_job_summary.set_finished()
                yield ingestion_job_summary

                # Start a new one
                is_first_chunk = False
                ingestion_job_summary = IngestionJobSummary.new(ingestion_job=self)
                self._last_progress_saved_at = 0
                self._save_progress(store, ingestion_job_summary, force=True)

        if ingestion_job_summary.task_count() > 0 or is_first_chunk:
            # When there is interesting information to store, or there was no data at all, store it
            ingestion_job_summary.set_finished()
            yield ingestion_job_summary

    def _execute_async(
        self,
        source,
        batches,
        store: DatasetStore,
        task_executor: TaskExecutor,
        last_modified_at_map,
        ingestion_job_summary: IngestionJobSummary,
        is_first_chunk: bool,
    ) -> Iterator[IngestionJobSummary]:
        """Execute using the submit/collect pattern for async sources."""

        def filtered_stream():
            """Lazily yield filtered DatasetResources across all batches."""
            while True:
                try:
                    with ingestion_job_summary.record_timing("find_datasets"):
                        try:
                            batch = next(batches)
                        except StopIteration:
                            return
                except Exception as e:
                    logger.exception("Failed to fetch next batch")
                    ingestion_job_summary.set_exception(e)
                    return

                # Fast pre-check
                if last_modified_at_map:
                    pending = []
                    for dr in batch:
                        identifier = Identifier.create_from_selector(
                            self.selector, **dr.dataset_resource_id
                        )
                        ts = last_modified_at_map.get(identifier.key)
                        if ts is not None and dr.files:
                            max_mod = max(f.last_modified for f in dr.files.values())
                            if ts >= max_mod:
                                ingestion_job_summary.increase_skipped_tasks(1)
                                continue
                        pending.append(dr)
                    batch = pending

                if not batch:
                    continue

                # Store check: determine create vs update
                dataset_identifiers = [
                    Identifier.create_from_selector(
                        self.selector, **dr.dataset_resource_id
                    )
                    for dr in batch
                ]

                with ingestion_job_summary.record_timing("get_dataset_collection"):
                    dataset_collection = store.get_dataset_collection(
                        dataset_type=self.ingestion_plan.dataset_type,
                        provider=batch[0].provider,
                        selector=dataset_identifiers,
                    )

                for dr in batch:
                    identifier = Identifier.create_from_selector(
                        self.selector, **dr.dataset_resource_id
                    )
                    dataset = dataset_collection.get(identifier)
                    if dataset:
                        if self.ingestion_plan.fetch_policy.should_refetch(dataset, dr):
                            dr._existing_dataset = dataset
                            yield dr
                        else:
                            store.dispatch(DatasetSkipped(dataset=dataset))
                            ingestion_job_summary.increase_skipped_tasks(1)
                    else:
                        if self.ingestion_plan.fetch_policy.should_fetch(dr):
                            yield dr
                        else:
                            ingestion_job_summary.increase_skipped_tasks(1)

        resources = filtered_stream()
        done = False

        while not done or source.has_pending():
            done = source.submit(resources)

            for dataset_resource in source.collect():
                task_summary = self._store_async_result(dataset_resource, store)
                ingestion_job_summary.add_task_summaries([task_summary])

                # Live snapshot (throttled) so long-running submit/collect jobs
                # are observable while polling, instead of only at the very end.
                self._save_progress(store, ingestion_job_summary)

        if ingestion_job_summary.task_count() > 0 or is_first_chunk:
            ingestion_job_summary.set_finished()
            yield ingestion_job_summary

    def _store_async_result(
        self, dataset_resource: DatasetResource, store: DatasetStore
    ):
        """Store a dataset resource returned by collect()."""
        import uuid

        dataset_identifier = Identifier(**dataset_resource.dataset_resource_id)
        revision_source = RevisionSource(
            source_id=str(uuid.uuid1()), source_type=SourceType.TASK
        )

        existing_dataset = getattr(dataset_resource, "_existing_dataset", None)

        # The source signalled (via mark_failed) that it could not fetch this
        # resource: record a FAILED task and store nothing, so it is retried
        # next run but stays visible in the summary. Mirror the operation that
        # would have run — UPDATE for an existing dataset, CREATE otherwise.
        if dataset_resource.fetch_error is not None:
            logger.warning(
                "Fetch failed for %s: %s",
                dataset_identifier,
                dataset_resource.fetch_error,
            )
            operation = Operation.UPDATE if existing_dataset else Operation.CREATE
            return TaskSummary.failed(str(uuid.uuid1()), dataset_identifier, operation)

        # Load files that have file_loader or json_content
        files = {}
        for file_id, file_resource in dataset_resource.files.items():
            files[file_id] = load_file(
                file_resource,
                dataset=existing_dataset,
                dataset_resource=dataset_resource,
            )

        if existing_dataset:
            with TaskSummary.update(
                str(uuid.uuid1()), dataset_identifier=dataset_identifier
            ) as task_summary:
                dataset_resource.run_post_load_files(files, existing_dataset)
                try:
                    revision = store.update_dataset(
                        dataset=existing_dataset,
                        name=dataset_resource.name,
                        state=dataset_resource.state,
                        metadata=dataset_resource.metadata,
                        files=files,
                        revision_source=revision_source,
                    )
                    task_summary.set_stats_from_revision(revision)
                except Exception as e:
                    raise SaveError("Could not update dataset") from e
        else:
            with TaskSummary.create(
                str(uuid.uuid1()), dataset_identifier
            ) as task_summary:
                dataset_resource.run_post_load_files(files)
                try:
                    revision = store.create_dataset(
                        dataset_type=dataset_resource.dataset_type,
                        provider=dataset_resource.provider,
                        dataset_identifier=dataset_identifier,
                        name=dataset_resource.name,
                        state=dataset_resource.state,
                        metadata=dataset_resource.metadata,
                        files=files,
                        revision_source=revision_source,
                    )
                    task_summary.set_stats_from_revision(revision)
                except Exception as e:
                    raise SaveError("Could not create dataset") from e

        return task_summary
