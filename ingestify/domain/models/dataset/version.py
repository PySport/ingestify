from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class DatasetVersion:
    modified_at: datetime
    tag: str
    size: int
    row_count: Optional[int] = None

