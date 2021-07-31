from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from .content import DatasetContent


@dataclass
class DatasetVersion:
    modified_at: datetime
    tag: str

    content: Optional[DatasetContent] = None
    content_id: Optional[str] = None


@dataclass
class DraftDatasetVersion:
    modified_at: datetime
    tag: str
    size: int
    content_type: str

    stream: IO[AnyStr]




