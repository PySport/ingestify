# Design: single-run lock per ingestion job

Status: **draft for review** · Area: `application` / `infra.store`

## Problem

ingestify assumes an `(IngestionPlan, Selector)` job runs in **one process at a time**.
Nothing enforces it. A scheduler (e.g. Cloud Run Job + Cloud Scheduler) or a restart of a
crashed container can start a second execution of the same job while the first is still
running — Cloud Run Jobs have no "max 1 execution" setting.

Two runs of the same job identity produce **undefined results**: they discover and write
the same datasets concurrently, and stateful sources double their work. Observed with a
paid, task-based async source: each run took an independent server-side snapshot at startup,
missed the other's just-submitted tasks, and **re-submitted (paid for) the same work**. The
`ingestion_job_summary` table also accumulated rows stuck in `RUNNING` from crashed
overlapping runs.

Because concurrent same-job execution is never well-defined, this is **not a knob** to
expose — it is an **invariant** the framework should uphold.

## Decision

ingestify enforces, best-effort, that **one job identity never runs in two processes at
once**. Always on, not configurable. Where the metadata store can provide a cross-process
lock (Postgres) it is enforced hard; where it cannot (SQLite / local single-process) it is
a documented no-op.

Not a semaphore, not `max_concurrent_runs: N` — a plain **mutex per job identity**. Parallel
partitions are still possible the correct way: give them **different selectors** → different
identities → different locks.

### Naming

Call it a **run lock** / single-run guarantee. Do **not** call it "concurrency":
`Source.max_concurrency` already exists and means something unrelated — the number of
worker processes for tasks *within* one job (`Loader.run` → `TaskExecutor(processes=...)`,
`loader.py:276`).

## Job identity (lock scope)

`(source.name, dataset_type, selector.key)` — exactly the merge key the loader already uses
to guarantee one dataset per combination (`loader.py:193-197`). Serialised to a stable
string:

```
job_key = f"{source.name}:{dataset_type}:{selector.key}"
```

Different selector → different `job_key` → runs in parallel. Same identity → competes for
the one lock.

## Mechanism — backend session lock (mutex)

The metadata store already owns a SQLAlchemy engine (`SqlAlchemySessionProvider.engine`,
`repository.py:95`) and already branches on dialect (`repository.py:171`). Both supported
server databases provide a **session-scoped** lock — held for the life of one connection,
released automatically when it ends:

- **PostgreSQL** — `pg_try_advisory_lock(:key)` / `pg_advisory_unlock(:key)`, with
  `key = signed_bigint(hash64(job_key))` (advisory locks take a bigint).
- **MySQL** — `GET_LOCK(:name, 0)` / `RELEASE_LOCK(:name)`, with `name = hex(hash(job_key))`
  (user-level lock names are ≤ 64 chars, so hash to a bounded string). `GET_LOCK(…, 0)` =
  try once, non-blocking: `1` = obtained, `0` = held by another session.

Same flow for both, on a **dedicated** connection (not the `scoped_session`, which is
reused/closed):

- try the lock → obtained → keep the connection open for the whole run.
- not obtained → another process holds it → the caller skips.
- release explicitly at the end + close the connection; on crash the connection drops and
  the server **releases the lock automatically** — no stale locks, no cleanup job.

Why a session lock: atomic (no "list executions then decide" race), self-healing (dies with
the connection), zero extra infra (reuses the metadata DB).

### Lock lifetime — released the moment the process stops

The lock lives exactly as long as its **connection/session**, nothing more. On any process
stop — clean exit, exception, `SIGKILL`, OOM-kill, container recycle — the socket closes and
the server (Postgres advisory lock or MySQL `GET_LOCK`, both session-scoped) releases the
lock **immediately**. No TTL, no heartbeat, no lease renewal, no cleanup job. That is the
whole reason to prefer this over a "lock row with an expiry" table, which would either block
forever after a crash or need a reaper.

One caveat, stated honestly: release depends on the DB *seeing* the connection close. A hard
network partition that leaves the socket half-open (client vanishes without a FIN) is the
single case where release waits on TCP keepalive instead of being instant — bounded by the
server's keepalive settings, never indefinite. For a job process that simply ends, the
socket closes and release is prompt. Consequence for the implementation: the dedicated lock
connection must **not** silently auto-reconnect (a reconnect is a *new* session, without the
lock) — a dropped lock connection is fatal to the run.

## API surface (grounded in the current code)

**`DatasetRepository`** (`domain/models/dataset/dataset_repository.py`, ABC) — new method:

```python
@abstractmethod
def acquire_run_lock(self, job_key: str) -> "RunLock | None":
    """Return a held RunLock, or None if another process holds this job_key.
    Stores without cross-process locking return an always-acquired no-op lock."""
```

**`RunLock`** — tiny handle:

```python
class RunLock:
    def release(self) -> None: ...
```

**`SqlAlchemyDatasetRepository`** (`infra/store/dataset/sqlalchemy/repository.py:208`):

```python
def acquire_run_lock(self, job_key: str) -> RunLock | None:
    dialect = self.dialect.name
    conn = self.session_provider.engine.connect()   # dedicated, outside the scoped_session
    if dialect == "postgresql":
        key = _signed_bigint(job_key)               # advisory locks take a bigint
        got = conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": key}).scalar()
        acquire, unlock = bool(got), text("SELECT pg_advisory_unlock(:k)")
    elif dialect == "mysql":
        name = _lock_name(job_key)                   # hex hash, <= 64 chars
        got = conn.execute(text("SELECT GET_LOCK(:n, 0)"), {"n": name}).scalar()
        acquire, unlock = (got == 1), text("SELECT RELEASE_LOCK(:n)")
    else:
        conn.close()
        return _NoopRunLock()                        # no cross-process lock (e.g. SQLite)
    if not acquire:
        conn.close()
        return None
    return _SessionRunLock(conn, unlock, params)     # holds conn; release() unlocks + closes
```

**`DatasetStore`** (`application/dataset_store.py:112`) delegates, exactly like
`save_ingestion_job_summary` (`dataset_store.py:194`):

```python
def acquire_run_lock(self, job_key: str) -> RunLock | None:
    return self.dataset_repository.acquire_run_lock(job_key)
```

**`IngestionJob.execute`** (`domain/models/ingestion/ingestion_job.py:283`) — acquire at the
very top, before the `RUNNING` summary is created/persisted (`:290`):

```python
job_key = f"{self.ingestion_plan.source.name}:{self.ingestion_plan.dataset_type}:{self.selector.key}"
lock = store.acquire_run_lock(job_key)
if lock is None:
    summary = IngestionJobSummary.new(ingestion_job=self)
    summary.set_skipped(reason="another run holds the lock")
    store.save_ingestion_job_summary(summary)
    yield summary
    return
try:
    ...  # existing execute body (state=RUNNING, submit/collect, set_finished)
finally:
    lock.release()
```

`execute` is a generator fully consumed by `Loader.run`, so the lock is held across the
whole run and released in `finally` on normal completion, exception, or `GeneratorExit`.

**`IngestionJobSummary`** — add a `SKIPPED` state + `set_skipped(reason)` (sits alongside
the existing `set_finished()`), so a skipped run is observable and distinct from RUNNING /
FINISHED. A skipped job does **not** create a RUNNING row.

## Behaviour on conflict

Skip: record a `SKIPPED` summary, do not run, let `Loader.run` continue to the next job.
The process exits **0** — a scheduler must not see a skip as a failure or retry it.

## Edge cases & failure modes

- **Crash mid-run** → connection drops → lock frees → next run proceeds. (Summary row stays
  RUNNING until Phase 2; correctness unaffected.)
- **Lock connection lost but process alive** (DB blip) → the run is now unprotected. Treat a
  dead lock connection as fatal: abort rather than continue.
- **Store without a session lock** (e.g. SQLite) → no cross-process guarantee (no-op,
  documented). Local runs are single-process anyway.
- **Multi-primary cluster** (Galera / multiple write nodes) → both `pg_advisory_lock` and
  MySQL `GET_LOCK` are *per node*, not cluster-wide, so two runs on two write nodes would not
  see each other. The common single-primary deployment is fully covered; documented as a
  limitation for multi-primary setups.
- **hash collision** between two identities → they'd share one lock (over-restrict, never
  under-restrict). Safe direction; negligible probability. Documented, not mitigated.

## Testing

- Two connections to a test **Postgres or MySQL**: first `acquire_run_lock(k)` holds; second
  returns `None`; after `release()` the second succeeds. Same test parametrised over both
  dialects (skips the one not available).
- Crash: close the holder connection → next acquire succeeds (auto-release).
- Store without a session lock (SQLite) → always grants (no-op contract).
- Integration: two overlapping `IngestionJob.execute` for the same identity → one runs, the
  other yields a `SKIPPED` summary; different selectors → both run.

## Phase 2 (separate) — reconcile stale RUNNING

Advisory locks free on crash, but `ingestion_job_summary` rows stay `RUNNING`. On acquiring
a lock, a `RUNNING` summary for the same `job_key` whose advisory lock is **not** currently
held (checked via `pg_locks`) is orphaned → mark `ABANDONED`/`FAILED`. Observability
cleanup, not correctness; keep out of phase 1.

## Backwards compatibility

Always-on is safe: concurrent same-job execution was already undefined, so enforcing it can
only prevent breakage. A run that previously "worked" under overlap by luck now gets a
clean `SKIPPED` instead of racing. Nothing to configure, nothing removed.

## Rollout

1. Implement (repository method + Postgres impl + `RunLock` + `DatasetStore` delegation +
   `IngestionJob.execute` integration + `SKIPPED` summary state + tests).
2. Release ingestify (version bump).
3. Consumers bump the dep; a scheduler that would otherwise start an overlapping run now
   self-skips instead of needing a manual pause.

## Open questions

1. `SKIPPED` as a first-class summary state (proposed) vs. just not persisting anything for
   a skipped run. First-class is better for observability.
2. Acquire inside `IngestionJob.execute` (proposed — owns the lifecycle) vs. in `Loader.run`
   around it. Either works; `execute` keeps it next to the RUNNING/summary logic.
