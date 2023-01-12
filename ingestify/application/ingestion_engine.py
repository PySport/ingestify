import logging
from typing import Dict, Optional

from ingestify.domain.models.event import EventBus, EventRepository, EventWriter
from ingestify.domain.models import (
    Source,
    dataset_repository_factory,
    file_repository_factory,
)

from .loader import Loader
from .dataset_store import DatasetStore
from ..domain import Selector, Dataset

logger = logging.getLogger(__name__)


class EventLogger:
    def dispatch(self, event):
        pass
        #logger.info(f"Got event: {event}")


class IngestionEngine:
    def __init__(self, dataset_url: str, file_url: str, sources: Dict[str, Source]):
        file_repository = file_repository_factory.build_if_supports(url=file_url)
        # dataset_repository = SqlAlchemyDatasetRepository("sqlite:///:memory:")
        dataset_repository = dataset_repository_factory.build_if_supports(
            url=dataset_url
        )

        # Note: disconnect event from loading. Event should only be used for
        #       metadata and 'loaded_files' for the actual data.
        event_bus = EventBus()
        event_repository = EventRepository()
        event_bus.register(EventWriter(event_repository))
        event_bus.register(EventLogger())

        store = DatasetStore(
            dataset_repository=dataset_repository,
            file_repository=file_repository,
            event_bus=event_bus,
        )
        self.sources = sources
        self.store = store
        self.loader = Loader(self.sources, self.store)

    def add_selector(self, source: str, selector: Dict):
        self.loader.add_selector(source, selector)

    def load(self):
        self.loader.collect_and_run()

    def get_dataset_collection(
        self,
        dataset_type: Optional[str] = None,
        provider: Optional[str] = None,
        selector: Optional[Selector] = None,
        **kwargs
    ):
        return self.store.get_dataset_collection(
            dataset_type=dataset_type, provider=provider, selector=selector, **kwargs
        )

    def load_with_kloppy(self, dataset: Dataset, **kwargs):
        files = self.store.load_files(dataset)
        if dataset.provider == "statsbomb":
            from kloppy import statsbomb
            return statsbomb.load(
                event_data=files['events.json'].stream,
                lineup_data=files['lineups.json'].stream,
                **kwargs
            )


