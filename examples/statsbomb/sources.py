import base64
import json
from typing import Optional, Dict

import requests

from ingestify import Source, retrieve_http
from ingestify.domain import Identifier, Version, DraftFile

BASE_URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"


class StatsbombGithub(Source):
    provider = "statsbomb"
    dataset_type = "event"

    def discover_datasets(self, competition_id: str, season_id: str):
        matches = requests.get(
            f"{BASE_URL}/matches/{competition_id}/{season_id}.json"
        ).json()
        datasets = []
        for match in matches:
            dataset = dict(match_id=match["match_id"], _match=match, _metadata=match)
            datasets.append(dataset)
        return datasets

    def fetch_dataset_files(self, identifier, current_version):
        current_files = current_version.modified_files_map if current_version else {}
        files = {}
        for filename, url in [
            ("lineups.json", f"{BASE_URL}/lineups/{identifier.match_id}.json"),
            ("events.json", f"{BASE_URL}/events/{identifier.match_id}.json"),
        ]:
            files[filename] = retrieve_http(url, current_files.get(filename))

        files["match.json"] = json.dumps(identifier._match)

        return files


class Wyscout(Source):
    provider = "wyscout"
    dataset_type = "event"

    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password

    def _get(self, path: str, version: str = "v3"):
        response = requests.get(
            f"https://apirest.wyscout.com/{version}/{path}",
            auth=(self.username, self.password),
        )
        response.raise_for_status()
        return response.json()

    def discover_datasets(self, season_id: int):
        matches = self._get(f"seasons/{season_id}/matches")
        datasets = []
        for match in matches["matches"]:
            dataset = dict(match_id=match["matchId"], version="v3", _metadata=match)
            datasets.append(dataset)

        return datasets

    def fetch_dataset_files(
        self, identifier, current_version
    ) -> Dict[str, Optional[DraftFile]]:
        current_files = current_version.modified_files_map if current_version else {}
        files = {}

        for filename, url in [
            (
                "events.json",
                f"https://apirest.wyscout.com/v3/"
                f"matches/{identifier.match_id}/events?fetch=teams,players",
            ),
        ]:
            files[filename] = retrieve_http(
                url, current_files.get(filename), auth=(self.username, self.password)
            )
        return files
