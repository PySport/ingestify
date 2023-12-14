from typing import TYPE_CHECKING

from .domain_event import DomainEvent

if TYPE_CHECKING:
    from ingestify.domain.models.dataset.events import (
        DatasetCreated,
        DatasetUpdated,
        RevisionAdded,
    )


class Subscriber:
    def __init__(self, store):
        self.store = store

    def on_dataset_created(self, event: "DatasetCreated"):
        pass

    def on_dataset_updated(self, event: "DatasetUpdated"):
        pass

    def on_version_added(self, event: "RevisionAdded"):
        pass

    def handle(self, event: DomainEvent):
        # TODO: fix the circular dependencies
        from ingestify.domain.models.dataset.events import (
            DatasetCreated,
            DatasetUpdated,
            RevisionAdded,
        )

        if isinstance(event, DatasetCreated):
            self.on_dataset_created(event)
        elif isinstance(event, DatasetUpdated):
            self.on_dataset_updated(event)
        elif isinstance(event, RevisionAdded):
            self.on_version_added(event)
