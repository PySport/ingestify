"""A source can signal, in the async submit/collect flow, that it could not
fetch a particular DatasetResource by calling ``dataset_resource.mark_failed()``
and yielding it from ``collect()``. ingestify then records a FAILED task (no
dataset is stored, so it is retried next run) instead of the failure being
silently dropped.

These tests assert on what is actually stored in the database.
"""
from typing import Iterator

from ingestify import Source, DatasetResource
from ingestify.domain.models.dataset.revision import RevisionState
from ingestify.domain.models.ingestion.ingestion_job_summary import IngestionJobState
from ingestify.domain.models.task.task_summary import Operation, TaskState
from ingestify.main import get_dev_engine
from ingestify.utils import utcnow


class AsyncSourceWithFailure(Source):
    """Async source whose collect() marks a (mutable) set of keywords failed."""

    provider = "fake_async"

    def __init__(self, name, keywords, failing_keywords=(), capacity=2):
        super().__init__(name)
        self._keywords = keywords
        self.failing_keywords = set(failing_keywords)
        self._capacity = capacity
        self._submitted = {}
        self._in_flight = 0
        # Bumped per successful fetch so a re-fetch yields fresh content (a new
        # revision), instead of identical bytes that collapse to NotModified.
        self._version = 0

    def find_datasets(
        self, dataset_type, data_spec_versions, dataset_collection_metadata, **kwargs
    ):
        for keyword in self._keywords:
            yield DatasetResource(
                dataset_resource_id={"keyword": keyword},
                provider=self.provider,
                dataset_type="keyword",
                name=keyword,
            )

    def submit(self, dataset_resources: Iterator[DatasetResource]) -> bool:
        for resource in dataset_resources:
            self._submitted[resource.dataset_resource_id["keyword"]] = resource
            self._in_flight += 1
            if self._in_flight >= self._capacity:
                return False
        return True

    def collect(self) -> Iterator[DatasetResource]:
        for keyword, resource in list(self._submitted.items()):
            if keyword in self.failing_keywords:
                resource.mark_failed("API returned PERMISSION_DENIED")
            else:
                self._version += 1
                resource.add_file(
                    last_modified=utcnow(),
                    data_feed_key="data",
                    data_spec_version="v1",
                    json_content={"keyword": keyword, "v": self._version},
                )
            del self._submitted[keyword]
            self._in_flight -= 1
            yield resource

    def has_pending(self) -> bool:
        return self._in_flight > 0


def _dev_engine(source, tmp_path):
    return get_dev_engine(
        source=source,
        dataset_type="keyword",
        data_spec_versions={"default": "v1"},
        dev_dir=str(tmp_path),
        configure_logging=False,
    )


def _datasets(engine):
    return list(
        engine.store.get_dataset_collection(
            provider="fake_async", dataset_type="keyword"
        )
    )


def test_mark_failed_sets_fetch_error():
    dr = DatasetResource(
        dataset_resource_id={"keyword": "x"},
        provider="p",
        dataset_type="keyword",
        name="x",
    )
    assert dr.fetch_error is None
    assert dr.mark_failed("boom") is dr  # chainable
    assert dr.fetch_error == "boom"


def test_failed_fetch_recorded_as_failed_task(tmp_path):
    engine = _dev_engine(
        AsyncSourceWithFailure("s", ["a", "b", "c"], failing_keywords={"b"}),
        tmp_path,
    )

    engine.run()

    summaries = engine.store.dataset_repository.load_ingestion_job_summaries()
    assert len(summaries) == 1
    summary = summaries[0]

    assert summary.state == IngestionJobState.FINISHED
    assert summary.successful_tasks == 2
    assert summary.failed_tasks == 1

    # The one persisted child row is the failed fetch, for the right resource.
    # It never existed before, so it is recorded as a failed CREATE.
    assert len(summary.task_summaries) == 1
    failed = summary.task_summaries[0]
    assert failed.state == TaskState.FAILED
    assert failed.operation == Operation.CREATE
    assert failed.dataset_identifier["keyword"] == "b"


def test_failed_fetch_stores_no_dataset(tmp_path):
    engine = _dev_engine(
        AsyncSourceWithFailure("s", ["a", "b", "c"], failing_keywords={"b"}),
        tmp_path,
    )

    engine.run()

    # Only the two successful keywords produced a dataset; "b" did not.
    stored_keywords = {ds.identifier["keyword"] for ds in _datasets(engine)}
    assert stored_keywords == {"a", "c"}


def test_failed_refetch_leaves_existing_dataset_untouched_and_recovers(tmp_path):
    """A failed *refetch* of an existing dataset must not corrupt it: nothing is
    stored, the existing revision stays current, the failure is a FAILED UPDATE
    task, and the resource still recovers on a later successful run.

    This locks in that mark_failed keeps the existing fetch/retry behaviour
    working — a failed fetch is never represented as a bad/failed dataset.
    """
    source = AsyncSourceWithFailure("s", ["b"])
    engine = _dev_engine(source, tmp_path)

    # Run 1: succeeds -> dataset "b" exists with a single revision.
    engine.run()
    dataset = _datasets(engine)[0]
    assert len(dataset.revisions) == 1

    # Force a refetch on the next run by invalidating the current revision.
    engine.store.invalidate_revision(dataset, reason="quality check failed")
    dataset = _datasets(engine)[0]
    assert dataset.current_revision.state == RevisionState.VALIDATION_FAILED

    # Run 2: the refetch fails. The existing dataset must be left exactly as it
    # was (still one revision, still VALIDATION_FAILED) and the failure recorded
    # as a FAILED UPDATE task (an existing dataset -> UPDATE, not CREATE).
    source.failing_keywords = {"b"}
    engine.run()

    dataset = _datasets(engine)[0]
    assert len(dataset.revisions) == 1, "failed refetch must not add a revision"
    assert dataset.current_revision.state == RevisionState.VALIDATION_FAILED

    summaries = engine.store.dataset_repository.load_ingestion_job_summaries()
    failed_summaries = [s for s in summaries if s.failed_tasks == 1]
    assert len(failed_summaries) == 1
    failed = failed_summaries[0].task_summaries[0]
    assert failed.state == TaskState.FAILED
    assert failed.operation == Operation.UPDATE

    # Run 3: succeeds again -> the resource recovers with a fresh revision and is
    # no longer VALIDATION_FAILED. Retry still works after a failure.
    source.failing_keywords = set()
    engine.run()

    dataset = _datasets(engine)[0]
    assert len(dataset.revisions) == 2
    assert dataset.current_revision.state != RevisionState.VALIDATION_FAILED
