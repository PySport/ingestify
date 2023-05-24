from typing import Optional, Dict

import requests

from ingestify import Source, retrieve_http
from ingestify.domain import DraftFile
from ingestify.exceptions import ConfigurationError

BASE_URL = "https://apirest.wyscout.com/v3"


class Wyscout(Source):
    provider = "wyscout"
    dataset_type = "event"

    def __init__(self, name: str, username: str, password: str):
        super().__init__(name)

        self.username = username.strip()
        self.password = password.strip()

        if not self.username:
            raise ConfigurationError(f"Username of Wyscout source named '{self.name}' cannot be empty")

        if not self.password:
            raise ConfigurationError(f"Username of Wyscout source named '{self.name}' cannot be empty")

    def _get(self, path: str):
        response = requests.get(
            BASE_URL + path,
            auth=(self.username, self.password),
        )
        if response.status_code == 400:
            # What if the response isn't a json?
            error = response.json()['error']
            raise ConfigurationError(
                f"Check username/password of Wyscout source named '{self.name}'. API response "
                f"was '{error['message']}' ({error['code']})."
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
                BASE_URL + f"/matches/{identifier.match_id}/events?fetch=teams,players",
            ),
        ]:
            files[filename] = retrieve_http(
                url, current_files.get(filename), auth=(self.username, self.password)
            )
        return files
