from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine

from ingestify.infra.event_log.consumer import EventLogConsumer
from ingestify.infra.event_log.subscriber import EventLogSubscriber
from ingestify.infra.event_log.tables import get_tables
from ingestify.utils import utcnow


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_consumer() -> EventLogConsumer:
    consumer = EventLogConsumer("sqlite:///:memory:", reader_name="test")
    return consumer


def insert_events(consumer: EventLogConsumer, *events):
    """Insert (event_type, payload) tuples directly into the event_log table."""
    with consumer._engine.connect() as conn:
        for event_type, payload in events:
            conn.execute(
                consumer._event_log_table.insert().values(
                    event_type=event_type,
                    payload_json=payload,
                    created_at=utcnow(),
                )
            )
        conn.commit()


def make_subscriber() -> EventLogSubscriber:
    engine = create_engine("sqlite:///:memory:")
    mock_store = MagicMock()
    mock_store.dataset_repository.session_provider.table_prefix = ""
    mock_store.dataset_repository.session_provider.engine = engine
    return EventLogSubscriber(mock_store)


# ---------------------------------------------------------------------------
# Consumer tests
# ---------------------------------------------------------------------------


def test_processes_events_in_order():
    consumer = make_consumer()
    insert_events(
        consumer,
        ("dataset_created", {"dataset_id": "a"}),
        ("revision_added", {"dataset_id": "b"}),
        ("revision_added", {"dataset_id": "c"}),
    )

    processed = []
    consumer._run_once(lambda et, p: processed.append(p["dataset_id"]))

    assert processed == ["a", "b", "c"]


def test_cursor_advanced_after_each_event():
    consumer = make_consumer()
    insert_events(
        consumer,
        ("dataset_created", {"dataset_id": "a"}),
        ("revision_added", {"dataset_id": "b"}),
    )

    cursors = []
    original = consumer._update_cursor

    def capture(conn, event_id):
        cursors.append(event_id)
        original(conn, event_id)

    consumer._update_cursor = capture
    consumer._run_once(lambda et, p: None)

    assert len(cursors) == 2
    assert cursors[0] < cursors[1]


def test_cursor_not_advanced_on_error():
    consumer = make_consumer()
    insert_events(consumer, ("dataset_created", {"dataset_id": "a"}))

    cursors = []
    original = consumer._update_cursor

    def capture(conn, event_id):
        cursors.append(event_id)
        original(conn, event_id)

    consumer._update_cursor = capture
    exit_code = consumer._run_once(
        lambda et, p: (_ for _ in ()).throw(RuntimeError("boom"))
    )

    assert exit_code == 1
    assert cursors == []


def test_no_events_returns_zero():
    consumer = make_consumer()
    exit_code = consumer._run_once(lambda et, p: None)
    assert exit_code == 0


def test_only_new_events_processed_after_cursor():
    consumer = make_consumer()
    insert_events(
        consumer,
        ("dataset_created", {"dataset_id": "a"}),
        ("revision_added", {"dataset_id": "b"}),
    )

    # consume first batch
    consumer._run_once(lambda et, p: None)

    # insert a new event
    insert_events(consumer, ("revision_added", {"dataset_id": "c"}))

    processed = []
    consumer._run_once(lambda et, p: processed.append(p["dataset_id"]))

    assert processed == ["c"]


# ---------------------------------------------------------------------------
# Subscriber tests
# ---------------------------------------------------------------------------


def make_dataset(dataset_id="ds1", provider="test"):
    dataset = MagicMock()
    dataset.dataset_id = dataset_id
    dataset.provider = provider
    dataset.model_dump.return_value = {"dataset_id": dataset_id, "provider": provider}
    return dataset


def make_event(event_type, dataset):
    event = MagicMock()
    type(event).event_type = event_type
    event.dataset = dataset
    return event


def test_subscriber_writes_event():
    subscriber = make_subscriber()
    subscriber.on_dataset_created(make_event("dataset_created", make_dataset()))

    with subscriber._engine.connect() as conn:
        rows = conn.execute(subscriber._event_log_table.select()).fetchall()

    assert len(rows) == 1
    assert rows[0].event_type == "dataset_created"
    assert rows[0].dataset_id == "ds1"


def test_subscriber_writes_all_event_types():
    subscriber = make_subscriber()
    dataset = make_dataset()

    subscriber.on_dataset_created(make_event("dataset_created", dataset))
    subscriber.on_revision_added(make_event("revision_added", dataset))
    subscriber.on_metadata_updated(make_event("metadata_updated", dataset))

    with subscriber._engine.connect() as conn:
        rows = conn.execute(subscriber._event_log_table.select()).fetchall()

    assert [r.event_type for r in rows] == [
        "dataset_created",
        "revision_added",
        "metadata_updated",
    ]
