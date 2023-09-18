from dataclasses import dataclass
from typing import Optional, List

from ingestify.domain.models import Source, Selector
from ingestify.domain.models.fetch_policy import FetchPolicy


@dataclass
class ExtractJob:
    source: Source
    selectors: List[Selector]
    fetch_policy: FetchPolicy
    dataset_type: str
