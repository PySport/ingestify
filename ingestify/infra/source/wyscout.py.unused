import json
from typing import Optional, Dict, List

import requests

from ingestify import Source, retrieve_http
from ingestify.domain import DraftFile
from ingestify.exceptions import ConfigurationError

BASE_URL = "https://apirest.wyscout.com/v3"


def wyscout_pager_fn(url, response):
    if response["meta"]["page_current"] < response["meta"]["page_count"]:
        return f"{url}&page={response['meta']['page_current'] + 1}"
    else:
        return None


class Wyscout(Source):
    def discover_selectors(self, dataset_type: str) -> List[Dict]:
        raise NotImplementedError("Not implemented for Wyscout")

    provider = "wyscout"

    def __init__(self, name: str, username: str, password: str):
        super().__init__(name)

        self.username = username.strip()
        self.password = password.strip()

        if not self.username:
            raise ConfigurationError(
                f"Username of Wyscout source named '{self.name}' cannot be empty"
            )

        if not self.password:
            raise ConfigurationError(
                f"Username of Wyscout source named '{self.name}' cannot be empty"
            )

    def _get(self, path: str):
        response = requests.get(
            BASE_URL + path,
            auth=(self.username, self.password),
        )
        if response.status_code == 400:
            # What if the response isn't a json?
            error = response.json()["error"]
            raise ConfigurationError(
                f"Check username/password of Wyscout source named '{self.name}'. API response "
                f"was '{error['message']}' ({error['code']})."
            )

        response.raise_for_status()
        return response.json()

    def _get_paged(self, path: str, data_path: str):
        data = []
        current_page = 1
        page_count = None
        while page_count is None or current_page <= page_count:
            page_data = self._get(path + f"?page={current_page}&limit=100")
            page_count = page_data["meta"]["page_count"]

            data.extend(page_data[data_path])
            current_page += 1

        return data

    def discover_datasets(self, dataset_type: str, season_id: int):
        matches = self._get(f"/seasons/{season_id}/matches")
        datasets = []
        for match in matches["matches"]:
            dataset = dict(match_id=match["matchId"], version="v3", _metadata=match)
            datasets.append(dataset)

        return datasets

    def fetch_dataset_files(
        self, dataset_type, identifier, current_version
    ) -> Dict[str, Optional[DraftFile]]:
        current_files = current_version.modified_files_map if current_version else {}
        files = {}

        for filename, url in [
            (
                "events.json",
                f"{BASE_URL}/matches/{identifier.match_id}/events?fetch=teams,players",
            ),
        ]:
            files[filename] = retrieve_http(
                url, current_files.get(filename), auth=(self.username, self.password)
            )
        return files


#
# class WyscoutEvent(Wyscout):
#     dataset_type = "event"
#
#     def discover_datasets(self, season_id: int):
#         matches = self._get(f"/seasons/{season_id}/matches")
#         datasets = []
#         for match in matches["matches"]:
#             dataset = dict(match_id=match["matchId"], version="v3", _metadata=match)
#             datasets.append(dataset)
#
#         return datasets
#
#     def fetch_dataset_files(
#         self, identifier, current_version
#     ) -> Dict[str, Optional[DraftFile]]:
#         current_files = current_version.modified_files_map if current_version else {}
#         files = {}
#
#         for filename, url in [
#             (
#                 "events.json",
#                 f"{BASE_URL}/matches/{identifier.match_id}/events?fetch=teams,players",
#             ),
#         ]:
#             files[filename] = retrieve_http(
#                 url, current_files.get(filename), auth=(self.username, self.password)
#             )
#         return files
#
#
# class WyscoutPlayer(Wyscout):
#     dataset_type = "player"
#
#     def discover_datasets(self, season_id: int):
#         return [
#             dict(
#                 version="v3",
#             )
#         ]
#
#     def fetch_dataset_files(
#         self, identifier, current_version
#     ) -> Dict[str, Optional[DraftFile]]:
#         current_files = current_version.modified_files_map if current_version else {}
#
#         return {
#             "players.json": retrieve_http(
#                 f"{BASE_URL}/seasons/{identifier.season_id}/players?limit=100",
#                 current_files.get("players.json"),
#                 pager=("players", wyscout_pager_fn),
#                 auth=(self.username, self.password),
#             )
#         }


if __name__ == "__main__":
    import dotenv, os

    dotenv.load_dotenv()

    kilmarnock_id = 8516
    competition_id = 750
    season_id = 188105
    match_id = 5459107
    player_id = 840543

    data = requests.get(
        f"{BASE_URL}/competitions/{competition_id}/players",
        # f"{BASE_URL}/players/{player_id}/career",
        # f"{BASE_URL}/matches/{match_id}/advancedstats/players",
        # f"{BASE_URL}/competitions/{competition_id}/matches",  # teams/{kilmarnock_id}/advancedstats?compId={competition_id}",
        # f"{BASE_URL}/teams/{kilmarnock_id}/squad", #teams/{kilmarnock_id}/advancedstats?compId={competition_id}",
        auth=(os.environ["WYSCOUT_USERNAME"], os.environ["WYSCOUT_PASSWORD"]),
    ).json()
    from pprint import pprint

    pprint(data)
