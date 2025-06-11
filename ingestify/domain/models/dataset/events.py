from typing import ClassVar

from pydantic import BaseModel

from ingestify.domain.models.event.domain_event import DomainEvent
from .dataset import Dataset
from .selector import Selector


class DatasetCreated(DomainEvent):
    dataset: Dataset
    event_type: ClassVar[str] = "dataset_created"


class RevisionAdded(DomainEvent):
    dataset: Dataset
    event_type: ClassVar[str] = "revision_added"


class MetadataUpdated(DomainEvent):
    dataset: Dataset
    event_type: ClassVar[str] = "metadata_updated"


class SelectorSkipped(DomainEvent):
    model_config = {"arbitrary_types_allowed": True}
    
    selector: Selector
    event_type: ClassVar[str] = "selector_skipped"


class DatasetSkipped(DomainEvent):
    dataset: Dataset
    event_type: ClassVar[str] = "dataset_skipped"
