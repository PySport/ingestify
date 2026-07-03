"""The IngestionJobSummary is persisted *live*: a RUNNING row is written as
soon as the job starts and updated while it runs — for both the sync
find_datasets flow and the async submit/collect flow. Previously the summary
was only written once, at the very end (so a long submit/collect run left no
trace at all until it finished, and none if it never finished).
"""
from typing import Iterator

from ingestify import Source, DatasetResource
from ingestify.domain.models.ingestion import ingestion_job as ingestion_job_module
from ingestify.domain.models.ingestion.ingestion_job_summary import IngestionJobState
from ingestify.main import get_dev_engine
from ingestify.utils import utcnow


class SyncSource(Source):
    """Plain source: find_datasets only (the batch/find_datasets flow)."""

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


def _spy_on_summary_saves(engine):
    """Record a snapshot of each save_ingestion_job_summary call.

    Snapshots are taken at call time because the summary object is mutated
    afterwards (recount / _set_ended).
    """
    calls = []
    original = engine.store.save_ingestion_job_summary

    def wrapper(summary, include_task_summaries=True):
        calls.append(
            {
                "state": summary.state,
                "include_task_summaries": include_task_summaries,
                "total_tasks": summary.total_tasks,
                "successful_tasks": summary.successful_tasks,
                "summary_id": summary.ingestion_job_summary_id,
            }
        )
        return original(summary, include_task_summaries=include_task_summaries)

    engine.store.save_ingestion_job_summary = wrapper
    return calls


def _dev_engine(source, tmp_path):
    return get_dev_engine(
        source=source,
        dataset_type="keyword",
        data_spec_versions={"default": "v1"},
        dev_dir=str(tmp_path),
        configure_logging=False,
    )


def test_sync_flow_writes_running_row_before_finished(tmp_path, monkeypatch):
    monkeypatch.setattr(ingestion_job_module, "PROGRESS_SAVE_INTERVAL", 1)
    engine = _dev_engine(SyncSource("test", ["a", "b", "c"]), tmp_path)
    calls = _spy_on_summary_saves(engine)

    engine.run()

    assert calls, "summary was never persisted"
    # First persisted write is the RUNNING row, parent-only (no task summaries).
    assert calls[0]["state"] == IngestionJobState.RUNNING
    assert calls[0]["include_task_summaries"] is False
    # Final write is FINISHED and carries the task summaries.
    assert calls[-1]["state"] == IngestionJobState.FINISHED
    assert calls[-1]["include_task_summaries"] is True
    assert calls[-1]["successful_tasks"] == 3
    # A single job -> a single summary id across all writes.
    assert {c["summary_id"] for c in calls} == {calls[0]["summary_id"]}


def test_async_flow_updates_summary_during_collect(tmp_path, monkeypatch):
    monkeypatch.setattr(ingestion_job_module, "PROGRESS_SAVE_INTERVAL", 1)
    engine = _dev_engine(
        AsyncSource("test", ["a", "b", "c", "d", "e"], capacity=2), tmp_path
    )
    calls = _spy_on_summary_saves(engine)

    engine.run()

    # RUNNING row written up front, before any task completed.
    assert calls[0]["state"] == IngestionJobState.RUNNING
    assert calls[0]["include_task_summaries"] is False
    # Progress is written *while polling*, not only at the end: at least one
    # RUNNING snapshot with tasks already counted precedes the final write.
    live_progress = [
        c
        for c in calls[:-1]
        if c["state"] == IngestionJobState.RUNNING and c["total_tasks"] > 0
    ]
    assert live_progress, "no live progress snapshot was written during collect"
    # Final write is FINISHED with all tasks accounted for.
    assert calls[-1]["state"] == IngestionJobState.FINISHED
    assert calls[-1]["successful_tasks"] == 5
