import asyncio
import inspect
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.utils import format_datetime, parsedate
from io import BytesIO
from typing import Dict, List, Optional

import requests
from domain.models import DatasetVersion
from infra.store.local_file_store import LocalFileStore

from ingestify import source_factory
from ingestify.source_base import (
    Dataset,
    File,
    DatasetIdentifier,
    DatasetSelector,
    DraftFile, FileNotModified,
    Source,
)
from utils import utcnow


def retrieve(url, current_file: Optional[File] = None) -> DraftFile:
    headers = {}
    if current_file:
        headers = {
            "if-modified-since": format_datetime(
                current_file.modified_at, usegmt=True
            )
        }
    response = requests.get(url, headers=headers)
    # ('ETag', 'W/"82587c5a4f85a76d68b26ed1278645b0a7f18441ee2e2a10f457f0f46b24e8e8"')

    if "last-modified" in response.headers:
        modified_at = parsedate(response.headers["last-modified"])
    else:
        modified_at = datetime.utcnow()

    tag = response.headers.get("etag")
    content_length = response.headers.get("content-length", 0)

    return DraftFile(
        modified_at=modified_at,
        tag=tag,
        size=int(content_length) if content_length else None,
        content_type=response.headers.get("content-type"),
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
                selector=dataset_selector, match_id=match["match_id"], _match=match
            )
            for match in matches
        ]

    def fetch_dataset_version(
        self,
        dataset_identifier: DatasetIdentifier,
        current_version: Optional[DatasetVersion],
    ) -> DatasetVersion:
        current_files = current_version.files if current_version else {}
        files = {}
        for file_name, url in [
            ("lineups.json", f"{BASE_URL}/lineups/{dataset_identifier.match_id}.json"),
            ("events.json", f"{BASE_URL}/events/{dataset_identifier.match_id}.json"),
        ]:
            files[file_name] = retrieve(
                url, current_files.get(file_name)
            )

        return DatasetVersion(
            created_at=utcnow(),
            description="Fetch",
            files=files
        )


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
        elif dataset.current_version.created_at > self.min_age:
            return True
        else:
            return False


def main():
    source = source_factory.build("StatsbombGithub")

    store = LocalFileStore("/tmp/blaat")

    fetch_policy = FetchPolicy()

    selector = DatasetSelector(competition_id=11, season_id=1)

    dataset_identifiers = source.fetch_dataset_identifiers(selector)
    dataset_collection = store.get_dataset_collection(selector)

    for dataset_identifier in dataset_identifiers:
        if dataset := dataset_collection.get(dataset_identifier):
            if fetch_policy.should_refetch(dataset):
                print(f"Updating {dataset_identifier}")
                draft_dataset_version = source.fetch_dataset_version(
                    dataset_identifier,
                    current_version=dataset.current_version
                )
                store.add_version(dataset, draft_dataset_version)
        else:
            if fetch_policy.should_fetch(dataset_identifier):
                draft_dataset_version = source.fetch_dataset_version(dataset_identifier)
                store.create_dataset(dataset_identifier, draft_dataset_version)


if __name__ == "__main__":
    main()
