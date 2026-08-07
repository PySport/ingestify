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
import pytest

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
