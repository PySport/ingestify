import asyncio
from dataclasses import dataclass
from typing import List, Dict, Optional

import aiohttp

from ingestify.source_base import (
    Source,
    BaseImportConfiguration,
    BaseDatasetDescriptor,
    Store,
)
from ingestify import source_factory


class VersionIdentifier:
    modified_at: datetime
    tag: str


async def retrieve(url, current_version: Optional[VersionIdentifier] = None):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            return await resp.json(content_type=None)


BASE_URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
class StatsbombGithub(Source):
    @dataclass
    class ImportConfiguration(BaseImportConfiguration):
        season_id: int
        competition_id: int
        type: str

    @dataclass
    class DatasetDescriptor(BaseDatasetDescriptor[ImportConfiguration]):
        match_id: int
        _match: Dict

    async def find_datasets(
        self, configuration: ImportConfiguration
    ) -> List[DatasetDescriptor]:

        matches = await retrieve(
            f"{BASE_URL}/matches/{configuration.competition_id}/{configuration.season_id}.json"
        )
        return [
            self.DatasetDescriptor(
                configuration=configuration,
                match_id=match['match_id'],
                _match=match
            )
            for match in matches
        ]

    async def retrieve_and_store_dataset(self, dataset_descriptor: DatasetDescriptor, store: Store):
        if metadata := await store.get_metadata(dataset_descriptor):
            fetch_if_changed()
        else:
            if dataset_descriptor.configuration.type == 'lineup':
                data = await retrieve(
                    f"{BASE_URL}/lineups/{dataset_descriptor.match_id}.json"
                )
            elif dataset_descriptor.configuration.type == 'events':
                data = await retrieve(
                    f"{BASE_URL}/events/{dataset_descriptor.match_id}.json"
                )
            else:
                raise Exception(f"Invalid dataset type {dataset_descriptor.configuration.type}")

            await store.add(
                dataset_descriptor,
                data
            )


def main():
    bla = source_factory.build(
        "StatsbombGithub",
        freshness_policy=AlwaysRefresh()
    )

    async def run():
        store = Store()

        dataset_descriptors = await bla.find_datasets(
            StatsbombGithub.ImportConfiguration(
                competition_id=11,
                season_id=1,
                type='lineup'
            )
        )
        for dataset_descriptor in dataset_descriptors:
            print(f"Retrieving {dataset_descriptor}")
            await bla.retrieve_and_store_dataset(dataset_descriptor, store)
        a = 1

    asyncio.run(run())

if __name__ == "__main__":
    main()
