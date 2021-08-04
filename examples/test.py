import json
import sys
from io import BytesIO
from typing import Dict, List, Optional

import requests
from application.syncer import sync_store
from domain.models import DatasetVersion
from infra import retrieve_http

from ingestify.source_base import DatasetIdentifier, DatasetSelector, DraftFile, Source

BASE_URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"


class StatsbombGithub(Source):
    def discover_datasets(
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

    def fetch_dataset_files(
        self,
        dataset_identifier: DatasetIdentifier,
        current_version: Optional[DatasetVersion],
    ) -> Dict[str, DraftFile]:
        current_files = current_version.files if current_version else {}
        files = {}
        for file_name, url in [
            ("lineups.json", f"{BASE_URL}/lineups/{dataset_identifier.match_id}.json"),
            ("events.json", f"{BASE_URL}/events/{dataset_identifier.match_id}.json"),
        ]:
            files[file_name] = retrieve_http(url, current_files.get(file_name))

        files["match.json"] = json.dumps(dataset_identifier._match)

        return files


def main():
    import logging

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    sync_store(
        "StatsbombGithub", dataset_selector=dict(competition_id=37, season_id=42)
    )


if __name__ == "__main__":
    main()
