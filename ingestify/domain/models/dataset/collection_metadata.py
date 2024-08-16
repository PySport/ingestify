from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class DatasetCollectionMetadata:
    last_modified: Optional[datetime]
    count: int
