import logging

from ingestify.domain.models.event import Subscriber
from ingestify.utils import utcnow

from .tables import get_tables

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
        tables = get_tables(session_provider.table_prefix)
        tables["metadata"].create_all(session_provider.engine, checkfirst=True)
        self._engine = session_provider.engine
        self._event_log_table = tables["event_log_table"]

    def _write(self, event_type: str, dataset) -> None:
        try:
            with self._engine.connect() as conn:
                conn.execute(
                    self._event_log_table.insert().values(
                        event_type=event_type,
                        payload_json=dataset.model_dump(
                            mode="json", exclude={"revisions"}
                        ),
                        source=dataset.provider,
                        dataset_id=dataset.dataset_id,
                        created_at=utcnow(),
                    )
                )
                conn.commit()
        except Exception:
            logger.exception(
                "EventLogSubscriber: failed to write event_type=%r dataset_id=%r",
                event_type,
                dataset.dataset_id,
            )

    def on_dataset_created(self, event) -> None:
        self._write(type(event).event_type, event.dataset)

    def on_metadata_updated(self, event) -> None:
        self._write(type(event).event_type, event.dataset)

    def on_revision_added(self, event) -> None:
        self._write(type(event).event_type, event.dataset)
