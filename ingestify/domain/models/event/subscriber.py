from typing import TYPE_CHECKING

from .domain_event import DomainEvent

if TYPE_CHECKING:
    from ingestify.domain.models.dataset.events import (
        DatasetCreated,
        MetadataUpdated,
        RevisionAdded,
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

    def handle(self, event: DomainEvent):
        # TODO: fix the circular dependencies
        from ingestify.domain.models.dataset.events import (
            DatasetCreated,
            MetadataUpdated,
            RevisionAdded,
        )

        if isinstance(event, DatasetCreated):
            self.on_dataset_created(event)
        elif isinstance(event, MetadataUpdated):
            self.on_metadata_updated(event)
        elif isinstance(event, RevisionAdded):
            self.on_revision_added(event)
