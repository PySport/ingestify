"""A zombie RUNNING summary -- left by a previous run that was hard-killed
(an uncatchable SIGKILL / OOM, or a SIGTERM that could not be persisted within
the grace window) before it could flip its own state -- must be reconciled to
FAILED by the next run of the same job identity.

This is the grace-independent backstop the in-process signal handlers cannot
provide: holding the single-run lock proves no live process owns the identity,
so any summary still RUNNING for it is dead. The dashboard therefore stops
reading a dead job as "still running".
"""
import uuid
from typing import Iterator

from ingestify import DatasetResource, Source
from ingestify.domain.models.ingestion.ingestion_job_summary import (
    IngestionJobSummary,
    IngestionJobState,
)
from ingestify.main import get_dev_engine


class SourceOk(Source):
    """A trivial async source whose run completes without error."""

    provider = "fake_async"

    def find_datasets(
        self, dataset_type, data_spec_versions, dataset_collection_metadata, **kwargs
    ):
        yield DatasetResource(
            dataset_resource_id={"keyword": "alpha"},
            provider=self.provider,
            dataset_type="keyword",
            name="alpha",
        )

    def submit(self, dataset_resources: Iterator[DatasetResource]) -> bool:
        list(dataset_resources)  # drain
        return True

    def collect(self) -> Iterator[DatasetResource]:
        yield from ()

    def has_pending(self) -> bool:
        return False


def _build_engine(tmp_path):
    return get_dev_engine(
        source=SourceOk("test"),
        dataset_type="keyword",
        data_spec_versions={"default": "v1"},
        dev_dir=str(tmp_path),
        configure_logging=False,
    )


def test_next_run_reconciles_zombie_running_to_failed(tmp_path):
    engine = _build_engine(tmp_path)

    # Run once to learn the exact job identity (provider / source / dataset_type
    # / selector) the engine produces.
    engine.run()
    first = engine.store.dataset_repository.load_ingestion_job_summaries()
    assert len(first) == 1
    identity = first[0]

    # Simulate a previous run that was hard-killed: an up-front RUNNING summary
    # for the SAME identity that was never flipped.
    zombie = IngestionJobSummary(
        ingestion_job_summary_id=str(uuid.uuid1()),
        ingestion_job_id="zombie-previous-run",
        provider=identity.provider,
        source_name=identity.source_name,
        dataset_type=identity.dataset_type,
        data_spec_versions=identity.data_spec_versions,
        selector=identity.selector,
        state=IngestionJobState.RUNNING,
    )
    engine.store.save_ingestion_job_summary(zombie)

    # A RUNNING summary is now present, waiting to be reconciled.
    running_before = engine.store.load_running_ingestion_job_summaries()
    assert [s.ingestion_job_summary_id for s in running_before] == [
        zombie.ingestion_job_summary_id
    ]

    # The next run of the same job reconciles it.
    engine.run()

    summaries = {
        s.ingestion_job_summary_id: s
        for s in engine.store.dataset_repository.load_ingestion_job_summaries()
    }
    reconciled = summaries[zombie.ingestion_job_summary_id]
    assert reconciled.state == IngestionJobState.FAILED
    assert reconciled.ended_at is not None

    # No zombie RUNNING rows remain.
    assert engine.store.load_running_ingestion_job_summaries() == []
