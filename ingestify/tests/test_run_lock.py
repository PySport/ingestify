"""Single-run lock — one (source, dataset_type, selector) job must never run in two
processes at once (design: docs/design/single-run-lock.md).

WHERE the lock lives, and WHY there:
  - In the metadata store (`DatasetRepository.acquire_run_lock`) — the only shared,
    cross-process state ingestify has. On a server DB it is a session-scoped lock held on a
    dedicated connection (Postgres `pg_advisory_lock`, MySQL `GET_LOCK`), so it is released
    the instant that connection (i.e. the process) ends: no TTL, no reaper. On stores without
    one (SQLite / local single-process) it is a documented no-op.
  - Taken in `IngestionJob.execute`, before the RUNNING summary is written, so a run that
    loses the race posts nothing and never shows up as RUNNING.

Session locks are a server-DB feature, so the exclusivity test runs against Postgres/MySQL
and skips on SQLite; the no-op contract is checked where no such lock exists.
"""
import threading

import pytest

from ingestify import DatasetResource, Source
from ingestify.domain import DataSpecVersionCollection
from ingestify.domain.models import Selector
from ingestify.domain.models.fetch_policy import FetchPolicy
from ingestify.domain.models.ingestion.ingestion_job_summary import IngestionJobState
from ingestify.domain.models.ingestion.ingestion_plan import IngestionPlan
from ingestify.main import get_engine
from ingestify.utils import utcnow

_LOCKING_DIALECTS = ("postgresql", "mysql")


def test_run_lock_is_exclusive_and_releases(engine):
    repo = engine.store.dataset_repository
    if repo.dialect.name not in _LOCKING_DIALECTS:
        pytest.skip("session-lock mutex needs Postgres or MySQL")

    first = repo.acquire_run_lock("source_a:type_x:{}")
    assert first is not None  # free -> acquired
    assert repo.acquire_run_lock("source_a:type_x:{}") is None  # 2nd session -> blocked

    first.release()
    again = repo.acquire_run_lock("source_a:type_x:{}")  # released -> free again
    assert again is not None

    other = repo.acquire_run_lock("source_b:type_y:{}")  # different job -> independent
    assert other is not None
    again.release()
    other.release()


def test_run_lock_is_noop_without_cross_process_store(engine):
    repo = engine.store.dataset_repository
    if repo.dialect.name in _LOCKING_DIALECTS:
        pytest.skip("covered by the exclusivity test")

    # No cross-process lock (e.g. SQLite): acquire must always grant, so a single local
    # process is never blocked by itself.
    a = repo.acquire_run_lock("x:y:{}")
    b = repo.acquire_run_lock("x:y:{}")
    assert a is not None and b is not None
    a.release()
    b.release()


class _FakeSource(Source):
    """One dataset. With ``entered``/``proceed`` it blocks inside the job (holding the run
    lock) until released; ``find_datasets_called`` records whether the job actually ran."""

    provider = "run_lock_fake"

    def __init__(self, name, entered=None, proceed=None):
        super().__init__(name)
        self._entered, self._proceed = entered, proceed
        self.find_datasets_called = False

    def find_datasets(
        self, dataset_type, data_spec_versions, dataset_collection_metadata, **kwargs
    ):
        self.find_datasets_called = True
        if self._entered is not None:
            self._entered.set()  # the run lock is held from here on
            self._proceed.wait(10)
        yield DatasetResource(
            dataset_resource_id={"keyword": "k"},
            provider=self.provider,
            dataset_type="keyword",
            name="k",
        ).add_file(
            last_modified=utcnow(),
            data_feed_key="data",
            data_spec_version="v1",
            json_content={"keyword": "k"},
        )


def _engine(db_url, file_dir, source):
    engine = get_engine(
        metadata_url=db_url,
        file_url=f"file://{file_dir}",
        bucket="main",
        disable_events=True,
    )
    dsv = DataSpecVersionCollection.from_dict({"default": "v1"})
    engine.add_ingestion_plan(
        IngestionPlan(
            source=source,
            dataset_type="keyword",
            selectors=[Selector.build({}, data_spec_versions=dsv)],
            fetch_policy=FetchPolicy(),
            data_spec_versions=dsv,
        )
    )
    return engine


def _summaries(engine):
    return engine.store.dataset_repository.load_ingestion_job_summaries()


def _cleanup(*engines):
    dropped = False
    for engine in engines:
        sp = engine.store.dataset_repository.session_provider
        if not dropped:
            sp.drop_all_tables()
            dropped = True
        sp.session.remove()
        sp.engine.dispose()


def test_two_instances_same_job_only_one_runs(ingestify_test_database_url, tmp_path):
    """The real thing: two ingestify instances over the SAME database try the SAME
    (source, dataset_type, selector) job at once. Instance 1 holds the run lock (it blocks
    inside the job); instance 2 must skip -- its source is never invoked -- and record a
    SKIPPED summary. Then instance 1 finishes normally. Needs a cross-process lock, so it
    runs on Postgres/MySQL and skips on SQLite."""
    if not ingestify_test_database_url.startswith(("postgres", "mysql")):
        pytest.skip("cross-process lock needs Postgres or MySQL")

    entered, proceed = threading.Event(), threading.Event()
    inst1 = _engine(
        ingestify_test_database_url, tmp_path, _FakeSource("shared", entered, proceed)
    )
    inst2_source = _FakeSource("shared")
    inst2 = _engine(ingestify_test_database_url, tmp_path, inst2_source)

    t1 = threading.Thread(target=inst1.run, daemon=True)
    t1.start()
    try:
        assert entered.wait(10), "instance 1 never entered the job"

        inst2.run()  # second instance, same job identity -> must skip
        # Decisive proof: instance 2 never ran the job body.
        assert inst2_source.find_datasets_called is False

        # Summaries live in the shared DB (both engines see all of them), so assert on the
        # state distribution, not on a per-engine read. Instance 1 is still blocked here:
        # exactly one SKIPPED (instance 2) and nothing FINISHED yet.
        states = [s.state for s in _summaries(inst2)]
        assert states.count(IngestionJobState.SKIPPED) == 1
        assert IngestionJobState.FINISHED not in states

        proceed.set()
        t1.join(10)
        states = [s.state for s in _summaries(inst1)]
        assert (
            states.count(IngestionJobState.FINISHED) == 1
        )  # instance 1 won and finished
        assert states.count(IngestionJobState.SKIPPED) == 1  # instance 2 stayed skipped
    finally:
        proceed.set()
        t1.join(10)
        _cleanup(inst1, inst2)
