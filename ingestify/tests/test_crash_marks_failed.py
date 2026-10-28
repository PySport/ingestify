"""A crash in a phase the inner handlers do not cover must mark the summary
FAILED, not leave it stuck at RUNNING.

The async submit/collect loop only handled StopProcessing / interrupts /
FatalError; an ordinary exception there (e.g. an HTTP 402 from the source API)
propagated uncaught, so the up-front RUNNING summary was never updated and
monitoring kept reading the job as "still running". execute() now has a
top-level backstop that flips any still-RUNNING summary to FAILED on any such
exception before re-raising.
"""
from typing import Iterator

import pytest

from ingestify import DatasetResource, Source
from ingestify.domain.models.ingestion.ingestion_job_summary import IngestionJobState
from ingestify.main import get_dev_engine
from ingestify.utils import utcnow


class SourceErrorInSubmit(Source):
    """Async source (submit/collect) that raises an ordinary exception from
    submit — mirrors a 402 Payment Required from the upstream API."""

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
        raise ValueError("402 Payment Required")

    def collect(self) -> Iterator[DatasetResource]:
        yield from ()

    def has_pending(self) -> bool:
        return False


def test_crash_in_submit_marks_summary_failed_not_running(tmp_path):
    engine = get_dev_engine(
        source=SourceErrorInSubmit("test"),
        dataset_type="keyword",
        data_spec_versions={"default": "v1"},
        dev_dir=str(tmp_path),
        configure_logging=False,
    )

    # The crash is a real failure, so it still propagates (the job exits non-zero).
    with pytest.raises(ValueError, match="402 Payment Required"):
        engine.run()

    # ...but the up-front RUNNING summary must now be flipped to FAILED, so no
    # zombie RUNNING row is left behind for monitoring to misread.
    summaries = engine.store.dataset_repository.load_ingestion_job_summaries()
    assert len(summaries) == 1
    assert summaries[0].state == IngestionJobState.FAILED
