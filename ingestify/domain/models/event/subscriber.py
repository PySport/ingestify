from typing import TYPE_CHECKING

from .domain_event import DomainEvent

if TYPE_CHECKING:
    from ingestify.domain.models.dataset.events import (
        DatasetCreated,
        MetadataUpdated,
        RevisionAdded,
        RevisionInvalidated,
    )


class Subscriber:
    def __init__(self, store):
        self.store = store

    def on_dataset_created(self, event: "DatasetCreated"):
        pass

    def on_metadata_updated(self, event: "MetadataUpdated"):
        pass

    def on_revision_added(self, event: "RevisionAdded"):
        pass

    def on_revision_invalidated(self, event: "RevisionInvalidated"):
        pass

    def handle(self, event: DomainEvent):
        # TODO: fix the circular dependencies
        from ingestify.domain.models.dataset.events import (
            DatasetCreated,
            MetadataUpdated,
            RevisionAdded,
            RevisionInvalidated,
        )

        if isinstance(event, DatasetCreated):
            self.on_dataset_created(event)
        elif isinstance(event, MetadataUpdated):
            self.on_metadata_updated(event)
        elif isinstance(event, RevisionAdded):
            self.on_revision_added(event)
        elif isinstance(event, RevisionInvalidated):
            self.on_revision_invalidated(event)

    def handle_many(self, events: list[DomainEvent]):
        """Handle a batch of events. Override for efficient bulk writes."""
        for event in events:
            self.handle(event)
