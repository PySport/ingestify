"""Tests for FatalError exception."""
from typing import Iterator
from unittest.mock import patch

import pytest

from ingestify import Source, DatasetResource
from ingestify.domain import DataSpecVersionCollection, DraftFile, Selector
from ingestify.domain.models.fetch_policy import FetchPolicy
from ingestify.domain.models.ingestion.ingestion_job_summary import IngestionJobState
from ingestify.domain.models.ingestion.ingestion_plan import IngestionPlan
from ingestify.exceptions import FatalError, StopProcessing
from ingestify.main import get_dev_engine
from ingestify.utils import utcnow


def good_loader(file_resource, current_file, **kwargs):
    return DraftFile.from_input("data", data_feed_key="f1")


class SourceFatalInFindDatasets(Source):
    """Source whose find_datasets fails fatally before yielding anything
    (mirrors an account-level authorization error surfacing in discovery)."""

    provider = "test_provider"

    def find_datasets(
        self, dataset_type, data_spec_versions, dataset_collection_metadata, **kwargs
    ):
        raise FatalError("account deactivated")
        yield  # pragma: no cover - makes this a generator


class SourceFatalMidStream(Source):
    """Source that yields 2 good datasets, then fails fatally."""

    provider = "test_provider"

    def find_datasets(
        self, dataset_type, data_spec_versions, dataset_collection_metadata, **kwargs
    ):
        for i in range(2):
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
                file_loader=good_loader,
            )
            yield r
        raise FatalError("account deactivated")


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


def test_fatal_error_has_exit_code():
    assert FatalError.exit_code == 1


def test_fatal_error_in_find_datasets_propagates(engine):
    """FatalError raised in find_datasets propagates out of engine.run()
    instead of being swallowed as a skipped find_datasets."""
    _setup(engine, SourceFatalInFindDatasets("s"))

    with pytest.raises(FatalError, match="deactivated"):
        engine.run()


def test_fatal_error_mid_stream_propagates(engine):
    """FatalError raised after some datasets still propagates."""
    _setup(engine, SourceFatalMidStream("s"))

    with pytest.raises(FatalError, match="deactivated"):
        engine.run()


def test_fatal_error_persists_failed_summary(engine):
    """FatalError must persist the summary (as FAILED) before aborting, so the
    failure isn't lost — the loader has no finally to save it otherwise."""
    _setup(engine, SourceFatalInFindDatasets("s"))

    with patch.object(engine.store, "save_ingestion_job_summary") as mock_save:
        with pytest.raises(FatalError):
            engine.run()

        assert mock_save.call_count >= 1, "summary was never saved on FatalError"
        saved = mock_save.call_args[0][0]
        assert saved.state == IngestionJobState.FAILED


# --- async submit/collect path -------------------------------------------------


class _AsyncSource(Source):
    """Async source that raises `error` from collect() after submitting."""

    provider = "fake_async"

    def __init__(self, name, keywords, error):
        super().__init__(name)
        self._keywords = keywords
        self._error = error
        self._in_flight = 0

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
        for _ in dataset_resources:
            self._in_flight += 1
        return True

    def collect(self) -> Iterator[DatasetResource]:
        raise self._error
        yield  # pragma: no cover - makes this a generator

    def has_pending(self) -> bool:
        return self._in_flight > 0


def _run_async(source, tmp_path):
    engine = get_dev_engine(
        source=source,
        dataset_type="keyword",
        data_spec_versions={"default": "v1"},
        dev_dir=str(tmp_path),
        configure_logging=False,
    )
    return engine


def test_fatal_error_in_async_collect_persists_failed_summary(tmp_path):
    """A FatalError raised while collecting (the submit/collect path bigintel
    uses) still persists a FAILED summary before aborting."""
    engine = _run_async(_AsyncSource("s", ["a", "b"], FatalError("boom")), tmp_path)

    with pytest.raises(FatalError):
        engine.run()

    summaries = engine.store.dataset_repository.load_ingestion_job_summaries()
    assert len(summaries) == 1
    assert summaries[0].state == IngestionJobState.FAILED


def test_stop_processing_in_async_collect_persists_summary(tmp_path):
    """StopProcessing while collecting is a controlled stop: the summary is
    persisted (FINISHED) instead of being lost, mirroring the sync path."""
    engine = _run_async(
        _AsyncSource("s", ["a", "b"], StopProcessing("quota")), tmp_path
    )

    with pytest.raises(StopProcessing):
        engine.run()

    summaries = engine.store.dataset_repository.load_ingestion_job_summaries()
    assert len(summaries) == 1
    assert summaries[0].state == IngestionJobState.FINISHED
