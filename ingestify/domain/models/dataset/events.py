from dataclasses import dataclass

from ingestify.domain.models.event import DomainEvent

from .dataset import Dataset


@dataclass
class DatasetCreated(DomainEvent):
    dataset: Dataset


@dataclass
class VersionAdded:
    pass
