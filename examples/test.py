import asyncio
import inspect
from dataclasses import dataclass
from typing import List, Dict, Optional

import requests

from domain.models import DatasetVersion
from ingestify.source_base import (
    Source,
    AbstractDatasetIdentifier,
    AbstractDatasetSelector,
    Store,
)
from ingestify import source_factory


class Dataset:
    pass


def retrieve(url, current_version: Optional[DatasetVersion] = None):
    return requests.get(url).json()


BASE_URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"

@dataclass
class DatasetSelector(AbstractDatasetSelector):
    season_id: int
    competition_id: int
    type: str


@dataclass
class DatasetIdentifier(AbstractDatasetIdentifier[DatasetSelector]):
    match_id: int
    _match: Dict


class StatsbombGithub(Source):
    def find_datasets(
        self, dataset_selector: DatasetSelector
    ) -> List[DatasetIdentifier]:

        matches = retrieve(
            f"{BASE_URL}/matches/{dataset_selector.competition_id}/{dataset_selector.season_id}.json"
        )
        return [
            DatasetIdentifier(
                dataset_selector=dataset_selector,
                match_id=match["match_id"],
                _match=match
            )
            for match in matches
        ]

    def fetch_dataset(
        self, dataset_identifier: DatasetIdentifier, current_version: DatasetVersion
    ) -> Dataset:
        if dataset_identifier.dataset_selector.type == "lineup":
            data = retrieve(f"{BASE_URL}/lineups/{dataset_identifier.match_id}.json")
        elif dataset_identifier.dataset_selector.type == "events":
            data = retrieve(f"{BASE_URL}/events/{dataset_identifier.match_id}.json")
        else:
            raise Exception(
                f"Invalid dataset type {dataset_identifier.dataset_selector.type}"
            )


def main():
    source = source_factory.build(
        "StatsbombGithub"
    )

    store = Store()

    refresh_policy = RefreshPolicy()


    selector = dict(
        competition_id=11,
        season_id=1,
        type="lineup"
    )

    dataset_identifiers = source.find_datasets(selector)
    current_datasets = store.get_datasets(selector)

    for dataset_identifier in dataset_identifiers:
        print(f"Retrieving {dataset_identifier}")
        source.fetch_dataset(dataset_identifier, store)
    a = 1


if __name__ == "__main__":
    main()
