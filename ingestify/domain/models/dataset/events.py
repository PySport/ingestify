from dataclasses import dataclass, field
from datetime import datetime

from ingestify.domain.models.event.domain_event import DomainEvent
from ingestify.utils import utcnow

from .dataset import Dataset


@dataclass
class DatasetCreated(DomainEvent):
    dataset: Dataset

    event_type: str = "dataset_created"
    occurred_at: datetime = field(default_factory=utcnow)


@dataclass
class RevisionAdded(DomainEvent):
    dataset: Dataset

    event_type: str = "revision_added"
    occurred_at: datetime = field(default_factory=utcnow)


@dataclass
class MetadataUpdated(DomainEvent):
    dataset: Dataset

    event_type: str = "metadata_updated"
    occurred_at: datetime = field(default_factory=utcnow)
