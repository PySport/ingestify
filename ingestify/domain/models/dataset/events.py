from pydantic import BaseModel

from ingestify.domain.models.event.domain_event import DomainEvent
from .dataset import Dataset


class DatasetCreated(DomainEvent):
    dataset: Dataset
    event_type: str = "dataset_created"


class RevisionAdded(DomainEvent):
    dataset: Dataset
    event_type: str = "revision_added"


class MetadataUpdated(DomainEvent):
    dataset: Dataset
    event_type: str = "metadata_updated"
