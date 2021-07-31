import asyncio
import inspect
from dataclasses import dataclass
from datetime import datetime, timedelta
from io import BytesIO
from typing import List, Dict, Optional

import requests

from domain.models import DatasetVersion
from infra.store.local_file_store import LocalFileStore
from ingestify.source_base import (
    Source,
    DatasetIdentifier,
    DatasetSelector,
    Dataset,
    DatasetContent,
    DraftDatasetVersion,
    Store,
)
from ingestify import source_factory


from email.utils import format_datetime, parsedate


def retrieve(url, current_version: Optional[DatasetVersion] = None):
    headers = {}
    if current_version:
        headers = {
            'if-modified-since': format_datetime(
                current_version.modified_at,
                usegmt=True
            )
        }
    response = requests.get(url, headers=headers)
    # ('ETag', 'W/"82587c5a4f85a76d68b26ed1278645b0a7f18441ee2e2a10f457f0f46b24e8e8"')

    if 'last-modified' in response.headers:
        modified_at = parsedate(response.headers['last-modified'])
    else:
        modified_at = datetime.utcnow()

    tag = response.headers.get('etag')
    content_length = response.headers.get('content-length', 0)

    return DraftDatasetVersion(
        modified_at=modified_at,
        tag=tag,
        size=int(content_length) if content_length else None,
        content_type=response.headers.get('content-type'),
        stream=BytesIO(response.content),
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
                selector=dataset_selector,
                match_id=match["match_id"],
                _match=match
            )
            for match in matches
        ]

    def fetch_dataset_version(
        self, dataset_identifier: DatasetIdentifier, current_version: Optional[DatasetVersion]
    ) -> DraftDatasetVersion:
        if dataset_identifier.selector.type == "lineup":
            dataset_version = retrieve(f"{BASE_URL}/lineups/{dataset_identifier.match_id}.json")
        elif dataset_identifier.selector.type == "events":
            dataset_version = retrieve(f"{BASE_URL}/events/{dataset_identifier.match_id}.json")
        else:
            raise Exception(
                f"Invalid dataset type {dataset_identifier.selector.type}"
            )

        return dataset_version


class FetchPolicy:
    def __init__(self):
        # refresh all data that changed less than a day ago
        self.min_age = datetime.utcnow() - timedelta(days=1)

    def should_fetch(self, dataset_identifier: DatasetIdentifier) -> bool:
        # this is called when dataset does not exist yet
        return True

    def should_refetch(self, dataset: Dataset) -> bool:
        if not dataset.versions:
            return True
        elif dataset.current_version.modified_at > self.min_age:
            return True
        else:
            return False


def main():
    source = source_factory.build(
        "StatsbombGithub"
    )

    store = LocalFileStore(
        "/tmp/blaat"
    )

    fetch_policy = FetchPolicy()

    selector = DatasetSelector(
        competition_id=11,
        season_id=1,
        type="lineup"
    )

    dataset_identifiers = source.fetch_dataset_identifiers(selector)
    dataset_collection = store.get_dataset_collection(selector)

    for dataset_identifier in dataset_identifiers:
        if dataset := dataset_collection.get(dataset_identifier):
            if fetch_policy.should_refetch(dataset):
                print(f"Updating {dataset_identifier}")
                dataset_version = source.fetch_dataset_version(
                    dataset_identifier,
                    current_version=dataset.current_version
                )
                store.add_version(
                    dataset,
                    dataset_version
                )
        else:
            if fetch_policy.should_fetch(dataset_identifier):
                dataset_version = source.fetch_dataset_version(
                    dataset_identifier
                )
                store.create_dataset(
                    dataset_identifier,
                    dataset_version
                )


if __name__ == "__main__":
    main()
