"""A SIGTERM mid-run (e.g. a Cloud Run task hitting its timeout) must mark the
summary ABORTED, not leave it stuck at RUNNING.

Python leaves SIGTERM on its default disposition -- terminate the process at
once, without raising anything -- so execute()'s
`except (KeyboardInterrupt, SystemExit)` never ran and a zombie RUNNING row was
left behind (monitoring kept reading the job as "still running"). loader.run()
now installs a SIGTERM handler (via abort_on_termination_signal) that raises
SystemExit, routing SIGTERM through the same graceful ABORTED path as Ctrl-C.
"""
import os
import signal
from typing import Iterator

import pytest

from ingestify import DatasetResource, Source
from ingestify.domain.models.ingestion.ingestion_job_summary import IngestionJobState
from ingestify.main import get_dev_engine


class SourceSigtermInSubmit(Source):
    """Async source (submit/collect) that sends SIGTERM to its own process from
    submit -- mirrors Cloud Run signalling a task that hit its timeout."""

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
        os.kill(os.getpid(), signal.SIGTERM)
        return True

    def collect(self) -> Iterator[DatasetResource]:
        yield from ()

    def has_pending(self) -> bool:
        return False


def test_sigterm_marks_summary_aborted_not_running(tmp_path):
    original_handler = signal.getsignal(signal.SIGTERM)

    engine = get_dev_engine(
        source=SourceSigtermInSubmit("test"),
        dataset_type="keyword",
        data_spec_versions={"default": "v1"},
        dev_dir=str(tmp_path),
        configure_logging=False,
    )

    # SIGTERM is translated to SystemExit, which still propagates (the task is
    # genuinely being terminated) -- but now gracefully, via the ABORTED path.
    with pytest.raises(SystemExit):
        engine.run()

    # The up-front RUNNING summary must be flipped to ABORTED, so no zombie
    # RUNNING row survives for monitoring to misread.
    summaries = engine.store.dataset_repository.load_ingestion_job_summaries()
    assert len(summaries) == 1
    assert summaries[0].state == IngestionJobState.ABORTED

    # The original disposition must be restored so the handler does not leak to
    # the rest of the process or to a subsequent run.
    assert signal.getsignal(signal.SIGTERM) is original_handler
