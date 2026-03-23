import logging
import time
from typing import Callable, Optional

from sqlalchemy import create_engine, select

from .event_log import EventLog
from .tables import get_tables

logger = logging.getLogger(__name__)


class EventLogConsumer:
    """Cursor-based consumer for the event_log table.

    Usage (run once, e.g. cron):
        EventLogConsumer.from_config("ingestify.yaml", reader_name="default").run(on_event)

    Usage (keep running, poll every 5 seconds):
        EventLogConsumer.from_config("ingestify.yaml", reader_name="default").run(on_event, poll_interval=5)

    Exit codes (returned by run):
        0  Batch processed successfully (or nothing new).
        1  A processing error occurred; cursor was NOT advanced.
    """

    def __init__(self, database_url: str, reader_name: str, table_prefix: str = ""):
        engine = create_engine(database_url)
        self._event_log = EventLog(engine, table_prefix)
        self._reader_name = reader_name
        self._engine = engine
        tables = get_tables(table_prefix)
        self._reader_state_table = tables["reader_state_table"]
        self._reader_state_table.create(engine, checkfirst=True)

    @classmethod
    def from_config(cls, config_file: str, reader_name: str) -> "EventLogConsumer":
        from pyaml_env import parse_config

        config = parse_config(config_file, default_value="")
        main = config["main"]
        table_prefix = main.get("metadata_options", {}).get("table_prefix", "")
        return cls(
            database_url=main["metadata_url"],
            reader_name=reader_name,
            table_prefix=table_prefix,
        )

    def _ensure_reader_state(self, conn) -> None:
        exists = conn.execute(
            select(self._reader_state_table.c.reader_name).where(
                self._reader_state_table.c.reader_name == self._reader_name
            )
        ).fetchone()
        if not exists:
            conn.execute(
                self._reader_state_table.insert().values(
                    reader_name=self._reader_name,
                    last_event_id=0,
                )
            )
            conn.commit()

    def _get_last_event_id(self, conn) -> int:
        row = conn.execute(
            select(self._reader_state_table.c.last_event_id).where(
                self._reader_state_table.c.reader_name == self._reader_name
            )
        ).fetchone()
        return row[0] if row else 0

    def _update_cursor(self, conn, event_id: int) -> None:
        conn.execute(
            self._reader_state_table.update()
            .where(self._reader_state_table.c.reader_name == self._reader_name)
            .values(last_event_id=event_id)
        )
        conn.commit()

    def _run_once(self, on_event: Callable, batch_size: int = 100) -> int:
        """Returns number of events processed, or -1 if a processing error occurred."""
        with self._engine.connect() as conn:
            self._ensure_reader_state(conn)
            last_id = self._get_last_event_id(conn)

        rows = self._event_log.fetch_batch(last_id, batch_size)

        with self._engine.connect() as conn:
            for event_id, event in rows:
                try:
                    on_event(event)
                except Exception:
                    logger.exception(
                        "Failed to process event id=%d type=%r — cursor NOT advanced",
                        event_id,
                        type(event).event_type,
                    )
                    return -1
                self._update_cursor(conn, event_id)

        return len(rows)

    def run(
        self,
        on_event: Callable,
        poll_interval: Optional[int] = None,
        batch_size: int = 100,
    ) -> int:
        while True:
            count = self._run_once(on_event, batch_size)
            if count < 0:
                return 1
            if count == 0:
                if poll_interval is None:
                    return 0
                time.sleep(poll_interval)
