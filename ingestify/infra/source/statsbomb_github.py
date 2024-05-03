import json
from datetime import datetime

import requests

from ingestify import Source, retrieve_http
from ingestify.domain import DraftFile
from ingestify.domain.models.dataset.dataset import DatasetState

BASE_URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"


class StatsbombGithub(Source):
    provider = "statsbomb"

    def discover_selectors(self, dataset_type: str, data_spec_versions: None = None):
        assert dataset_type == "match"

        competitions = requests.get(f"{BASE_URL}/competitions.json").json()
        return [
            dict(
                competition_id=competition["competition_id"],
                season_id=competition["season_id"],
            )
            for competition in competitions
        ]

    def discover_datasets(
        self,
        dataset_type,
        competition_id: str = None,
        season_id: str = None,
        data_spec_versions=None,
    ):
        assert dataset_type == "match"

        datasets = []

        matches = requests.get(
            f"{BASE_URL}/matches/{competition_id}/{season_id}.json"
        ).json()

        for match in matches:
            last_updated = match["last_updated"]
            if "Z" not in last_updated:
                # Assume UTC
                last_updated += "Z"

            last_modified = datetime.fromisoformat(last_updated.replace("Z", "+00:00"))

            dataset = dict(
                competition_id=competition_id,
                season_id=season_id,
                match_id=match["match_id"],
                _last_modified=last_modified,
                _match=match,
                _metadata=match,
                _state=DatasetState.COMPLETE,
            )
            datasets.append(dataset)
        return datasets

    def fetch_dataset_files(
        self, dataset_type, identifier, current_revision, data_spec_versions
    ):
        assert dataset_type == "match"

        current_files = current_revision.modified_files_map if current_revision else {}
        files = {}
        for filename, url in [
            ("lineups.json", f"{BASE_URL}/lineups/{identifier.match_id}.json"),
            ("events.json", f"{BASE_URL}/events/{identifier.match_id}.json"),
        ]:
            data_feed_key = filename.split(".")[0]
            file_id = data_feed_key + "__v1"
            files[file_id] = retrieve_http(
                url,
                current_files.get(filename),
                file_data_feed_key=data_feed_key,
                file_data_spec_version="v1",
                file_data_serialization_format="json",
            )

        files["match__v1"] = DraftFile.from_input(
            json.dumps(identifier._match, indent=4),
            data_feed_key="match",
            data_spec_version="v1",
            data_serialization_format="json",
            modified_at=None,
        )

        return files
