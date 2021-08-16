import json
from typing import Dict, List, Optional

import requests

from ingestify.infra import retrieve_http
from ingestify.source_base import DraftFile, Identifier, Source, Version

BASE_URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"


class StatsbombGithub(Source):
    provider = "statsbomb"
    dataset_type = "event"

    def discover_datasets(self, competition_id: str, season_id: str) -> List[Dict]:
        matches = requests.get(
            f"{BASE_URL}/matches/{competition_id}/{season_id}.json"
        ).json()
        return [dict(match_id=match["match_id"], _match=match) for match in matches]

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
