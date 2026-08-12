"""Tests for StopProcessing exception."""
from unittest.mock import patch

import pytest

from ingestify import Source, DatasetResource
from ingestify.domain import DataSpecVersionCollection, DraftFile, Selector
from ingestify.domain.models.dataset.collection_metadata import (
    DatasetCollectionMetadata,
)
from ingestify.domain.models.fetch_policy import FetchPolicy
from ingestify.domain.models.ingestion.ingestion_job_summary import IngestionJobState
from ingestify.domain.models.ingestion.ingestion_plan import IngestionPlan
from ingestify.exceptions import StopProcessing
from ingestify.utils import utcnow


def good_loader(file_resource, current_file, **kwargs):
    return DraftFile.from_input("data", data_feed_key="f1")


def stopping_loader(file_resource, current_file, **kwargs):
    raise StopProcessing("API quota exhausted")


def interrupting_loader(file_resource, current_file, **kwargs):
    raise KeyboardInterrupt()


class SourceWithStopProcessing(Source):
    """Source that yields 5 datasets. The 3rd one raises StopProcessing."""

    provider = "test_provider"
    bad_loader = staticmethod(stopping_loader)

    def find_datasets(
        self, dataset_type, data_spec_versions, dataset_collection_metadata, **kwargs
    ):
        for i in range(5):
            loader = self.bad_loader if i == 2 else good_loader
            r = DatasetResource(
                dataset_resource_id={"item_id": i},
                provider=self.provider,
                dataset_type="test",
                name=f"item-{i}",
            )
            r.add_file(
                last_modified=utcnow(),
                data_feed_key="f1",
                data_spec_version="v1",
                file_loader=loader,
            )
            yield r


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


def test_stop_processing_has_exit_code():
    assert StopProcessing.exit_code == 2


def test_stop_processing_propagates(engine):
    """StopProcessing raised by a loader propagates out of engine.run()."""
    _setup(engine, SourceWithStopProcessing("s"))

    with pytest.raises(StopProcessing, match="quota exhausted"):
        engine.run()


def test_stop_processing_preserves_completed_datasets(engine):
    """Datasets processed before StopProcessing are saved."""
    _setup(engine, SourceWithStopProcessing("s"))

    try:
        engine.run()
    except StopProcessing:
        pass

    datasets = list(
        engine.store.get_dataset_collection(
            provider="test_provider",
            dataset_type="test",
        )
    )
    assert len(datasets) == 2


def test_stop_processing_saves_ingestion_job_summary(engine):
    """IngestionJobSummary is saved even when StopProcessing occurs."""
    _setup(engine, SourceWithStopProcessing("s"))

    with patch.object(engine.store, "save_ingestion_job_summary") as mock_save:
        try:
            engine.run()
        except StopProcessing:
            pass

        assert (
            mock_save.call_count >= 1
        ), "save_ingestion_job_summary should be called even on StopProcessing"


class SourceWithInterrupt(SourceWithStopProcessing):
    """Same shape, but the 3rd dataset raises KeyboardInterrupt (Ctrl-C / SIGTERM)."""

    bad_loader = staticmethod(interrupting_loader)


def _states(engine):
    return [
        s.state for s in engine.store.dataset_repository.load_ingestion_job_summaries()
    ]


def test_stop_processing_marks_summary_aborted(engine):
    """A StopProcessing stop leaves the summary ABORTED (stopped early), not FINISHED."""
    _setup(engine, SourceWithStopProcessing("s"))

    with pytest.raises(StopProcessing):
        engine.run()

    assert _states(engine) == [IngestionJobState.ABORTED]


def test_keyboard_interrupt_marks_summary_aborted(engine):
    """Ctrl-C mid-run leaves the summary ABORTED instead of a stuck RUNNING row."""
    _setup(engine, SourceWithInterrupt("s"))

    with pytest.raises(KeyboardInterrupt):
        engine.run()

    assert _states(engine) == [IngestionJobState.ABORTED]
