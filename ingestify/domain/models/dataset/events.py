from dataclasses import dataclass

from ingestify.domain.models.event.domain_event import DomainEvent

from .dataset import Dataset


@dataclass
class DatasetCreated(DomainEvent):
    dataset: Dataset


@dataclass
class VersionAdded(DomainEvent):
    dataset: Dataset


@dataclass
class DatasetUpdated(DomainEvent):
    dataset: Dataset
