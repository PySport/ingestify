from datetime import datetime

import requests

from ingestify import Source, DatasetResource
from ingestify.domain.models.dataset.dataset import DatasetState

BASE_URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
DATA_SPEC_VERSION = "v1-open-data"


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

    def find_datasets(
        self,
        dataset_type: str,
        competition_id: str,
        season_id: str,
        match_id: str = None,
        data_spec_versions=None,
        dataset_collection_metadata=None,
    ):
        assert dataset_type == "match"

        matches = requests.get(
            f"{BASE_URL}/matches/{competition_id}/{season_id}.json"
        ).json()

        for match in matches:
            if match_id:
                if match["match_id"] != match_id:
                    continue

            last_modified = datetime.fromisoformat(match["last_updated"] + "+00:00")

            # Open data is always complete.. I guess?
            state = DatasetState.COMPLETE

            name = (
                f"{match['match_date']} / "
                f"{match['home_team']['home_team_name']} - {match['away_team']['away_team_name']}"
            )

            dataset_resource = DatasetResource(
                dataset_resource_id=dict(
                    competition_id=competition_id,
                    season_id=season_id,
                    match_id=match["match_id"],
                ),
                dataset_type=dataset_type,
                provider=self.provider,
                name=name,
                metadata=match,
                state=state,
            )

            dataset_resource.add_file(
                last_modified=last_modified,
                data_feed_key="match",
                data_spec_version=DATA_SPEC_VERSION,
                json_content=match,
            )

            if state.is_complete:
                name += f" / {match['home_score']}-{match['away_score']}"

                for data_feed_key in ["lineups", "events"]:
                    dataset_resource.add_file(
                        last_modified=last_modified,
                        data_feed_key=data_feed_key,
                        data_spec_version=DATA_SPEC_VERSION,
                        url=f"{BASE_URL}/{data_feed_key}/{match['match_id']}.json",
                        data_serialization_format="json",
                    )

                if (
                    match["last_updated_360"]
                    and match["match_status_360"] == "available"
                ):
                    dataset_resource.add_file(
                        last_modified=datetime.fromisoformat(
                            match["last_updated_360"] + "+00:00"
                        ),
                        data_feed_key="360-frames",
                        data_spec_version=DATA_SPEC_VERSION,
                        url=f"{BASE_URL}/three-sixty/{match['match_id']}.json",
                        data_serialization_format="json",
                        http_options={"ignore_not_found": True},
                    )

            yield dataset_resource
