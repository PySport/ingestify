from dataclasses import dataclass
from typing import List

from ingestify.domain.models import Source, Selector
from ingestify.domain.models.data_spec_version_collection import (
    DataSpecVersionCollection,
)
from ingestify.domain.models.fetch_policy import FetchPolicy


@dataclass
class ExtractJob:
    source: Source
    selectors: List[Selector]
    fetch_policy: FetchPolicy
    dataset_type: str
    data_spec_versions: DataSpecVersionCollection

    def __repr__(self):
        return f'<ExtractJob source="{self.source.name}" dataset_type="{self.dataset_type}">'

    def __str__(self):
        return repr(self)
