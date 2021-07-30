from dataclasses import dataclass
from datetime import datetime


@dataclass
class DatasetVersion:
    modified_at: datetime
    tag: str


@dataclass
class NotAvailable(DatasetVersion):
    modified_at = None
    tag = None
