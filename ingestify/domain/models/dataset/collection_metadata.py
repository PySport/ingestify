from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class DatasetCollectionMetadata:
    # This can be useful to figure out if a backfill is required
    first_modified: Optional[datetime]

    # Use the last modified to only retrieve datasets that are changed
    last_modified: Optional[datetime]
    row_count: int
