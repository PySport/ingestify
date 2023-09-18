import logging
from typing import Dict, Optional

from ingestify.domain.models.event import EventBus, EventRepository, EventWriter
from ingestify.domain.models import Source

from .loader import Loader
from .dataset_store import DatasetStore
from ..domain.models.extract_job import ExtractJob

logger = logging.getLogger(__name__)


class EventLogger:
    def dispatch(self, event):
        pass
        # logger.info(f"Got event: {event}")


class IngestionEngine:
    def __init__(self, store: DatasetStore):

        # Note: disconnect event from loading. Event should only be used for
        #       metadata and 'loaded_files' for the actual data.
        event_bus = EventBus()
        event_repository = EventRepository()
        event_bus.register(EventWriter(event_repository))
        event_bus.register(EventLogger())

        self.store = store
        self.loader = Loader(self.store)

    def add_extract_job(self, extract_job: ExtractJob):
        self.loader.add_extract_job(extract_job)

    def load(self):
        self.loader.collect_and_run()
