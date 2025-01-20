from typing import ClassVar

from pydantic import BaseModel

from ingestify.domain.models.event.domain_event import DomainEvent
from .dataset import Dataset


class DatasetCreated(DomainEvent):
    dataset: Dataset
    event_type: ClassVar[str] = "dataset_created"


class RevisionAdded(DomainEvent):
    dataset: Dataset
    event_type: ClassVar[str] = "revision_added"


class MetadataUpdated(DomainEvent):
    dataset: Dataset
    event_type: ClassVar[str] = "metadata_updated"
