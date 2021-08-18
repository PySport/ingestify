from dataclasses import dataclass
from typing import Dict

from ingestify.domain.models.event import DomainEvent

from .dataset import Dataset, DraftFile


@dataclass
class DatasetCreated(DomainEvent):
    dataset: Dataset
    files: Dict[str, DraftFile]
    description: str


@dataclass
class VersionAdded:
    pass
