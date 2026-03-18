from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine

from ingestify.domain.models.dataset.dataset import Dataset, DatasetState
from ingestify.domain.models.dataset.events import DatasetCreated, RevisionAdded
from ingestify.infra.event_log.consumer import EventLogConsumer
from ingestify.infra.event_log.event_log import EventLog
from ingestify.infra.event_log.subscriber import EventLogSubscriber
from ingestify.utils import utcnow


@pytest.fixture
def dataset():
    return Dataset(
        bucket="main",
        dataset_id="ds1",
        name="test",
        state=DatasetState.COMPLETE,
        dataset_type="match",
        provider="test",
        identifier={"match_id": "1"},
        metadata={},
        created_at=utcnow(),
        updated_at=utcnow(),
        last_modified_at=None,
    )


@pytest.fixture
def event_log():
    return EventLog(create_engine("sqlite:///:memory:"))


@pytest.fixture
def consumer():
    return EventLogConsumer("sqlite:///:memory:", reader_name="test")


@pytest.fixture
def subscriber():
    engine = create_engine("sqlite:///:memory:")
    store = MagicMock()
    store.dataset_repository.session_provider.table_prefix = ""
    store.dataset_repository.session_provider.engine = engine
    return EventLogSubscriber(store)


def test_event_log_write_and_fetch(event_log, dataset):
    event_log.write(RevisionAdded(dataset=dataset))
    _, event = event_log.fetch_batch(0, 10)[0]
    assert isinstance(event, RevisionAdded)
    assert event.dataset.dataset_id == "ds1"


def test_consumer_processes_events(consumer, dataset):
    consumer._event_log.write(RevisionAdded(dataset=dataset))
    received = []
    consumer._run_once(lambda e: received.append(type(e)))
    assert received == [RevisionAdded]


def test_consumer_cursor_not_advanced_on_error(consumer, dataset):
    consumer._event_log.write(RevisionAdded(dataset=dataset))
    exit_code = consumer._run_once(lambda e: 1 / 0)
    assert exit_code == 1
    # Next run still sees the same event
    received = []
    consumer._run_once(lambda e: received.append(e))
    assert len(received) == 1


def test_subscriber_writes_event(subscriber, dataset):
    subscriber.on_revision_added(RevisionAdded(dataset=dataset))
    _, event = subscriber._event_log.fetch_batch(0, 10)[0]
    assert isinstance(event, RevisionAdded)
    assert event.dataset.dataset_id == "ds1"
