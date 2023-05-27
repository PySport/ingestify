import logging
from typing import Dict, Optional

from ingestify.domain.models.event import EventBus, EventRepository, EventWriter
from ingestify.domain.models import Source

from .loader import Loader
from .dataset_store import DatasetStore
from ..domain import Selector, Dataset
from ..domain.models.fetch_policy import FetchPolicy

logger = logging.getLogger(__name__)


class EventLogger:
    def dispatch(self, event):
        pass
        # logger.info(f"Got event: {event}")


class IngestionEngine:
    def __init__(self, store: DatasetStore, sources: Dict[str, Source]):

        # Note: disconnect event from loading. Event should only be used for
        #       metadata and 'loaded_files' for the actual data.
        event_bus = EventBus()
        event_repository = EventRepository()
        event_bus.register(EventWriter(event_repository))
        event_bus.register(EventLogger())

        self.sources = sources
        self.store = store
        self.loader = Loader(self.sources, self.store)

    def add_selector(self, source: str, selector: Dict, fetch_policy: FetchPolicy):
        self.loader.add_selector(source, selector, fetch_policy)

    def load(self):
        self.loader.collect_and_run()
