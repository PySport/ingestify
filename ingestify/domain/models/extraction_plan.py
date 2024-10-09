from dataclasses import dataclass
from typing import List

from ingestify.domain.models import Source, Selector
from ingestify.domain.models.data_spec_version_collection import (
    DataSpecVersionCollection,
)
from ingestify.domain.models.fetch_policy import FetchPolicy


@dataclass
class ExtractionPlan:
    source: Source
    selectors: List[Selector]
    fetch_policy: FetchPolicy
    dataset_type: str
    data_spec_versions: DataSpecVersionCollection
