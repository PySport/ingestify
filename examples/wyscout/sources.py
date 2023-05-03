from typing import Optional, Dict

import requests

from ingestify import Source, retrieve_http
from ingestify.domain import DraftFile

BASE_URL = "https://apirest.wyscout.com/v3"


class Wyscout(Source):
    provider = "wyscout"
    dataset_type = "event"

    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password

    def _get(self, path: str):
        response = requests.get(
            BASE_URL + path,
            auth=(self.username, self.password),
        )
        response.raise_for_status()
        return response.json()

    def discover_datasets(self, season_id: int):
        matches = self._get(f"/seasons/{season_id}/matches")
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
                BASE_URL +
                f"/matches/{identifier.match_id}/events?fetch=teams,players",
            ),
        ]:
            files[filename] = retrieve_http(
                url, current_files.get(filename), auth=(self.username, self.password)
            )
        return files
