import json
import sys
from os import environ
from typing import Dict, List, Optional

import requests
from application.syncer import Syncer
from domain.models import Version
from infra import retrieve_http

from ingestify.source_base import DraftFile, Identifier, Selector, Source

BASE_URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"


class StatsbombGithub(Source):
    provider = "statsbomb"
    dataset_type = "event"

    def discover_datasets(
        self, competition_id: str, season_id: str
    ) -> List[Dict]:
        matches = requests.get(f"{BASE_URL}/matches/{competition_id}/{season_id}.json").json()
        return [
            dict(
                match_id=match["match_id"],
                _match=match
            )
            for match in matches
        ]

    def fetch_dataset_files(
        self,
        identifier: Identifier,
        current_version: Optional[Version],
    ) -> Dict[str, DraftFile]:
        current_files = current_version.modified_files_map if current_version else {}
        files = {}
        for filename, url in [
            ("lineups.json", f"{BASE_URL}/lineups/{identifier.match_id}.json"),
            ("events.json", f"{BASE_URL}/events/{identifier.match_id}.json"),
        ]:
            files[filename] = retrieve_http(url, current_files.get(filename))

        files["match.json"] = json.dumps(identifier._match)

        return files


def main():
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    data = requests.get('https://raw.githubusercontent.com/statsbomb/open-data/master/data/competitions.json').json()

    syncer = Syncer(
        database_url=environ['DATABASE_URL']
    )
    for competition in data:
        syncer.add_selector(
            source_name='StatsbombGithub',
            selector=dict(
                competition_id=competition['competition_id'],
                season_id=competition['season_id']
            )
        )
    #syncer.add_job("StatsbombGithub", dict(competition_id=37, season_id=42))
    #syncer.add_job("StatsbombGithub", dict(competition_id=11, season_id=1))
    syncer.collect_and_run()


if __name__ == "__main__":
    main()
