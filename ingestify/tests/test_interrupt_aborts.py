"""Interrupting a job during discovery must mark its summary ABORTED, not leave
it stuck at RUNNING.

The KeyboardInterrupt/SystemExit handlers only wrapped the task-execution phase;
an interrupt during metadata or find_datasets (Ctrl-C, or a Cloud Run SIGTERM
while discovering) propagated uncaught and left a zombie RUNNING summary. The
summary is persisted RUNNING up front, so it must be flipped to ABORTED whatever
phase is interrupted.
"""
import pytest

from ingestify import Source
from ingestify.domain import DataSpecVersionCollection, Selector
from ingestify.domain.models.fetch_policy import FetchPolicy
from ingestify.domain.models.ingestion.ingestion_job_summary import IngestionJobState
from ingestify.domain.models.ingestion.ingestion_plan import IngestionPlan


class SourceInterruptInFindDatasets(Source):
    """Raises the given interrupt while discovering (before yielding anything),
    mirroring a Ctrl-C / SIGTERM landing in find_datasets."""

    provider = "test_provider"

    def __init__(self, name, exc):
        super().__init__(name)
        self._exc = exc

    def find_datasets(
        self, dataset_type, data_spec_versions, dataset_collection_metadata, **kwargs
    ):
        raise self._exc
        yield  # pragma: no cover - makes this a generator


class SourceErrorInFindDatasets(Source):
    """Fails with an ordinary Exception while discovering."""

    provider = "test_provider"

    def find_datasets(
        self, dataset_type, data_spec_versions, dataset_collection_metadata, **kwargs
    ):
        raise ValueError("boom")
        yield  # pragma: no cover - makes this a generator


def _setup(engine, source):
    dsv = DataSpecVersionCollection.from_dict({"default": {"v1"}})
    engine.add_ingestion_plan(
        IngestionPlan(
            source=source,
            fetch_policy=FetchPolicy(),
            dataset_type="test",
            selectors=[Selector.build({}, data_spec_versions=dsv)],
            data_spec_versions=dsv,
        )
    )


@pytest.mark.parametrize("exc", [KeyboardInterrupt, SystemExit])
def test_interrupt_during_find_datasets_marks_summary_aborted(engine, exc):
    _setup(engine, SourceInterruptInFindDatasets("s", exc()))

    with pytest.raises(exc):
        engine.run()

    summaries = engine.store.dataset_repository.load_ingestion_job_summaries()
    assert len(summaries) == 1
    assert summaries[0].state == IngestionJobState.ABORTED


def test_ordinary_exception_in_find_datasets_is_failed_not_aborted(engine):
    """Only KeyboardInterrupt/SystemExit map to ABORTED. An ordinary Exception is
    a failure, so it must stay FAILED — the interrupt handler must not widen to
    catch it."""
    _setup(engine, SourceErrorInFindDatasets("s"))

    engine.run()  # ordinary exceptions are recorded, not re-raised

    summaries = engine.store.dataset_repository.load_ingestion_job_summaries()
    assert len(summaries) == 1
    assert summaries[0].state == IngestionJobState.FAILED
