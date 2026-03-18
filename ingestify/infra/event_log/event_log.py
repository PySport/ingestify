import json
import logging

from sqlalchemy import select

from ingestify.domain.models.dataset.dataset import Dataset
from ingestify.domain.models.dataset.events import (
    DatasetCreated,
    MetadataUpdated,
    RevisionAdded,
)
from ingestify.domain.models.event.domain_event import DomainEvent
from ingestify.utils import utcnow

from .tables import get_tables

logger = logging.getLogger(__name__)

_EVENT_TYPE_MAP = {
    "dataset_created": DatasetCreated,
    "revision_added": RevisionAdded,
    "metadata_updated": MetadataUpdated,
}


class EventLog:
    def __init__(self, engine, table_prefix: str = ""):
        tables = get_tables(table_prefix)
        self._engine = engine
        self._table = tables["event_log_table"]
        self._table.create(engine, checkfirst=True)

    def write(self, event: DomainEvent) -> None:
        dataset = event.dataset
        with self._engine.connect() as conn:
            conn.execute(
                self._table.insert().values(
                    event_type=type(event).event_type,
                    payload_json=dataset.model_dump(mode="json"),
                    source=dataset.provider,
                    dataset_id=dataset.dataset_id,
                    created_at=utcnow(),
                )
            )
            conn.commit()

    def fetch_batch(self, last_event_id: int, batch_size: int) -> list:
        """Returns a list of (event_id, domain_event) tuples."""
        with self._engine.connect() as conn:
            rows = conn.execute(
                select(
                    self._table.c.id,
                    self._table.c.event_type,
                    self._table.c.payload_json,
                )
                .where(self._table.c.id > last_event_id)
                .order_by(self._table.c.id)
                .limit(batch_size)
            ).fetchall()

        result = []
        for event_id, event_type, payload_json in rows:
            event_cls = _EVENT_TYPE_MAP.get(event_type)
            if event_cls is None:
                logger.debug(
                    "Skipping unknown event_type=%r (id=%d)", event_type, event_id
                )
                continue
            payload = (
                payload_json
                if isinstance(payload_json, dict)
                else json.loads(payload_json)
            )
            result.append(
                (event_id, event_cls(dataset=Dataset.model_validate(payload)))
            )

        return result
