import asyncio
import inspect
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Optional

import requests

from domain.models import DatasetVersion
from infra.store.local_file_store import LocalFileStore
from ingestify.source_base import (
    Source,
    DatasetIdentifier,
    DatasetSelector,
    Dataset,
    Store,
)
from ingestify import source_factory


from email.utils import formatdate, parsedate


def retrieve(url, current_version: Optional[DatasetVersion] = None):
    response = requests.get(url)
    # ('ETag', 'W/"82587c5a4f85a76d68b26ed1278645b0a7f18441ee2e2a10f457f0f46b24e8e8"')

    if 'last-modified' in response.headers:
        modified_at = parsedate(response.headers['last-modified'])
    else:
        modified_at = datetime.utcnow()

    tag = response.headers.get('etag')
    content_length = response.headers.get('content-length', 0)

    return (
        DatasetVersion(
            modified_at=modified_at,
            tag=tag,
            size=int(content_length) if content_length else None
        ),
        response.content
    )


BASE_URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"


class StatsbombGithub(Source):
    def fetch_dataset_identifiers(
        self, dataset_selector: DatasetSelector
    ) -> List[DatasetIdentifier]:
        url = dataset_selector.format_string(
            f"{BASE_URL}/matches/$competition_id/$season_id.json"
        )

        matches = requests.get(url).json()
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
            dataset_version, data = retrieve(f"{BASE_URL}/lineups/{dataset_identifier.match_id}.json")
        elif dataset_identifier.dataset_selector.type == "events":
            dataset_version, data = retrieve(f"{BASE_URL}/events/{dataset_identifier.match_id}.json")
        else:
            raise Exception(
                f"Invalid dataset type {dataset_identifier.dataset_selector.type}"
            )

        return Dataset(
            dataset_identifier=dataset_identifier,
            dataset_version=dataset_version,
            content=data
        )


def main():
    source = source_factory.build(
        "StatsbombGithub"
    )

    store = LocalFileStore(
        "/tmp/blaat"
    )

#    refresh_policy = RefreshPolicy()


    selector = DatasetSelector(
        competition_id=11,
        season_id=1,
        type="lineup"
    )

    dataset_identifiers = source.fetch_dataset_identifiers(selector)
    current_datasets = store.get_dataset_identifiers(selector)

    for dataset_identifier in dataset_identifiers:
        print(f"Retrieving {dataset_identifier}")
        source.fetch_dataset(dataset_identifier, store)
    a = 1


if __name__ == "__main__":
    main()
