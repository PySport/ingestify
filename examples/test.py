from dataclasses import dataclass
from typing import List

from ingestify.source_base import (
    Source,
    BaseImportConfiguration,
    BaseDatasetDescriptor,
    Store,
)


class StatsbombGithub(Source):
    @dataclass
    class ImportConfiguration(BaseImportConfiguration):
        season_id: str
        competition_id: str
        type: str

    @dataclass
    class DatasetDescriptor(BaseDatasetDescriptor):
        match_id: str

    async def find_datasets(
        self, configuration: ImportConfiguration
    ) -> List[DatasetDescriptor]:
        ds = self.DatasetDescriptor(configuration=configuration, match_id="None")
        return []

    async def store_dataset(self, dataset: DatasetDescriptor, store: Store):
        if metadata := store.get(dataset):
            fetch_if_changed()
