import json
from datetime import datetime

import requests

from ingestify import Source, retrieve_http

BASE_URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"


class StatsbombGithub(Source):
    provider = "statsbomb"

    def discover_selectors(self, dataset_type: str):
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
        self, dataset_type, competition_id: str = None, season_id: str = None
    ):
        assert dataset_type == "match"

        datasets = []

        matches = requests.get(
            f"{BASE_URL}/matches/{competition_id}/{season_id}.json"
        ).json()

        for match in matches:
            dataset = dict(
                competition_id=competition_id,
                season_id=season_id,
                match_id=match["match_id"],
                _last_modified=datetime.fromisoformat(
                    match["last_updated"].replace("Z", "+00:00")
                ),
                _match=match,
                _metadata=match,
            )
            datasets.append(dataset)
        return datasets

    def fetch_dataset_files(self, dataset_type, identifier, current_version):
        assert dataset_type == "event"

        current_files = current_version.modified_files_map if current_version else {}
        files = {}
        for filename, url in [
            ("lineups.json", f"{BASE_URL}/lineups/{identifier.match_id}.json"),
            ("events.json", f"{BASE_URL}/events/{identifier.match_id}.json"),
        ]:
            files[filename] = retrieve_http(url, current_files.get(filename))

        files["match.json"] = json.dumps(identifier._match)

        return files
