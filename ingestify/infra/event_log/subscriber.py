import logging

from ingestify.domain.models.event import Subscriber

from .event_log import EventLog

logger = logging.getLogger(__name__)


class EventLogSubscriber(Subscriber):
    """Persists every Ingestify dataset event to the event_log table.

    Uses the same database as the dataset store — no extra configuration needed.

    Register in ingestify.yaml:
        event_subscribers:
          - type: ingestify.infra.event_log.EventLogSubscriber
    """

    def __init__(self, store):
        super().__init__(store)
        session_provider = store.dataset_repository.session_provider
        self._event_log = EventLog(
            session_provider.engine, session_provider.table_prefix
        )

    def _write(self, event) -> None:
        try:
            self._event_log.write(event)
        except Exception:
            logger.exception(
                "EventLogSubscriber: failed to write event_type=%r dataset_id=%r",
                type(event).event_type,
                event.dataset.dataset_id,
            )

    def on_dataset_created(self, event) -> None:
        self._write(event)

    def on_metadata_updated(self, event) -> None:
        self._write(event)

    def on_revision_added(self, event) -> None:
        self._write(event)

    def on_revision_invalidated(self, event) -> None:
        self._write(event)
