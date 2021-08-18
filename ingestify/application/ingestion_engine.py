import logging
from typing import Dict

from ingestify.domain.models.event import EventBus, EventRepository, EventWriter
from ingestify.domain.models import (Source, dataset_repository_factory,
                                     file_repository_factory)

from .loader import Loader
from .dataset_store import DatasetStore

logger = logging.getLogger(__name__)


class EventLogger:
    def dispatch(self, event):
        logger.info(f"Got event: {event}")


class IngestionEngine:
    def __init__(self, dataset_url: str, file_url: str, sources: Dict[str, Source]):
        file_repository = file_repository_factory.build_if_supports(url=file_url)
        # dataset_repository = SqlAlchemyDatasetRepository("sqlite:///:memory:")
        dataset_repository = dataset_repository_factory.build_if_supports(
            url=dataset_url
        )

        event_bus = EventBus()
        event_repository = EventRepository()
        event_bus.register(EventWriter(event_repository))
        event_bus.register(EventLogger())

        store = DatasetStore(
            dataset_repository=dataset_repository,
            file_repository=file_repository,
            event_bus=event_bus
        )

        self.loader = Loader(sources, store)

    def add_selector(self, source: str, selector: Dict):
        self.loader.add_selector(source, selector)

    def load(self):
        self.loader.collect_and_run()