from dataclasses import dataclass
from typing import List

from ingestify.domain.models import Source, Selector
from ingestify.domain.models.data_format_collection import DataFormatCollection
from ingestify.domain.models.fetch_policy import FetchPolicy


@dataclass
class ExtractJob:
    source: Source
    selectors: List[Selector]
    fetch_policy: FetchPolicy
    dataset_type: str
    data_formats: DataFormatCollection
