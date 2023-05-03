import json

import requests

from ingestify import Source, retrieve_http

BASE_URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"


class StatsbombGithub(Source):
    provider = "statsbomb"
    dataset_type = "event"

    def discover_datasets(self, competition_id: str = None, season_id: str = None):
        datasets = []

        if not competition_id:
            competitions = requests.get(
                f"{BASE_URL}/competitions.json"
            ).json()
            seasons = [
                (competition['competition_id'], competition['season_id'])
                for competition in competitions
            ]
        else:
            seasons = [
                (competition_id, season_id)
            ]

        for competition_id, season_id in seasons:
            matches = requests.get(
                f"{BASE_URL}/matches/{competition_id}/{season_id}.json"
            ).json()

            for match in matches:
                dataset = dict(
                    competition_id=competition_id,
                    season_id=season_id,
                    match_id=match["match_id"],
                    _match=match,
                    _metadata=match
                )
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

