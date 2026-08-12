"""The IngestionJobSummary is persisted *live*: a RUNNING row is written to the
database as soon as the job starts and updated while it runs — for both the
sync find_datasets flow and the async submit/collect flow. Previously the
summary was written to the database only once, at the very end (so a long
submit/collect run left no trace until it finished, and none at all if it never
finished).

These tests assert on what is actually stored in the database
(load_ingestion_job_summaries), not just on the in-memory save calls.
"""
from typing import Iterator

from ingestify import Source, DatasetResource
from ingestify.domain.models.ingestion import ingestion_job as ingestion_job_module
from ingestify.domain.models.ingestion.ingestion_job_summary import IngestionJobState
from ingestify.main import get_dev_engine
from ingestify.utils import utcnow


class SyncSource(Source):
    """Plain source: find_datasets only (the batch / find_datasets flow)."""

    provider = "fake_sync"

    def __init__(self, name, keywords):
        super().__init__(name)
        self._keywords = keywords

    def find_datasets(
        self, dataset_type, data_spec_versions, dataset_collection_metadata, **kwargs
    ):
        for keyword in self._keywords:
            yield DatasetResource(
                dataset_resource_id={"keyword": keyword},
                provider=self.provider,
                dataset_type="keyword",
                name=keyword,
            ).add_file(
                last_modified=utcnow(),
                data_feed_key="data",
                data_spec_version="v1",
                json_content={"keyword": keyword},
            )


class AsyncSource(Source):
    """Source using the submit/collect (async) pattern."""

    provider = "fake_async"

    def __init__(self, name, keywords, capacity=2):
        super().__init__(name)
        self._keywords = keywords
        self._capacity = capacity
        self._submitted = {}
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
        for resource in dataset_resources:
            self._submitted[resource.dataset_resource_id["keyword"]] = resource
            self._in_flight += 1
            if self._in_flight >= self._capacity:
                return False
        return True

    def collect(self) -> Iterator[DatasetResource]:
        for keyword, resource in list(self._submitted.items()):
            resource.add_file(
                last_modified=utcnow(),
                data_feed_key="data",
                data_spec_version="v1",
                json_content={"keyword": keyword},
            )
            del self._submitted[keyword]
            self._in_flight -= 1
            yield resource

    def has_pending(self) -> bool:
        return self._in_flight > 0


def _db_summaries(engine):
    """Read the ingestion job summaries back from the database."""
    return engine.store.dataset_repository.load_ingestion_job_summaries()


def _capture_db_after_each_save(engine):
    """After every summary write, read the summaries back FROM THE DATABASE and
    snapshot their (state, total_tasks). Lets us assert on what is actually
    persisted mid-run — not merely on the in-memory save calls.
    """
    snapshots = []
    original = engine.store.save_ingestion_job_summary

    def wrapper(summary):
        result = original(summary)
        snapshots.append(
            [
                (s.state, s.total_tasks, s.successful_tasks)
                for s in _db_summaries(engine)
            ]
        )
        return result

    engine.store.save_ingestion_job_summary = wrapper
    return snapshots


def _dev_engine(source, tmp_path):
    return get_dev_engine(
        source=source,
        dataset_type="keyword",
        data_spec_versions={"default": "v1"},
        dev_dir=str(tmp_path),
        configure_logging=False,
    )


def test_sync_flow_persists_running_row_before_finished(tmp_path, monkeypatch):
    monkeypatch.setattr(ingestion_job_module, "PROGRESS_SAVE_INTERVAL", 1)
    engine = _dev_engine(SyncSource("test", ["a", "b", "c"]), tmp_path)
    db_snapshots = _capture_db_after_each_save(engine)

    engine.run()

    # The very first thing written to the DB is a RUNNING row.
    assert db_snapshots, "nothing was ever written to the database"
    first = db_snapshots[0]
    assert len(first) == 1
    assert first[0][0] == IngestionJobState.RUNNING

    # End state, read straight from the database.
    summaries = _db_summaries(engine)
    assert len(summaries) == 1
    summary = summaries[0]
    assert summary.state == IngestionJobState.FINISHED
    assert summary.successful_tasks == 3
    # Only failed task summaries are persisted; this run had none.
    assert summary.task_summaries == []


def test_async_flow_updates_db_summary_during_collect(tmp_path, monkeypatch):
    monkeypatch.setattr(ingestion_job_module, "PROGRESS_SAVE_INTERVAL", 1)
    engine = _dev_engine(
        AsyncSource("test", ["a", "b", "c", "d", "e"], capacity=2), tmp_path
    )
    db_snapshots = _capture_db_after_each_save(engine)

    engine.run()

    # RUNNING row is in the database from the start, before any task completed.
    assert db_snapshots[0] == [(IngestionJobState.RUNNING, 0, 0)]

    # While polling, the database already shows a RUNNING summary with tasks
    # counted — i.e. progress is visible before the job finishes.
    seen_live_progress = any(
        state == IngestionJobState.RUNNING and total > 0
        for snapshot in db_snapshots[:-1]
        for (state, total, _successful) in snapshot
    )
    assert seen_live_progress, "database never showed live progress during collect"

    # Final state in the database.
    summaries = _db_summaries(engine)
    assert len(summaries) == 1
    summary = summaries[0]
    assert summary.state == IngestionJobState.FINISHED
    assert summary.successful_tasks == 5
    assert summary.task_summaries == []
