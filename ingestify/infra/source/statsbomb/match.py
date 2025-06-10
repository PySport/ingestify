from datetime import datetime

from ingestify import DatasetResource
from ingestify.domain.models.dataset.dataset import DatasetState

from .base import StatsBombBaseAPI


class StatsBombMatchAPI(StatsBombBaseAPI):
    def discover_selectors(self, dataset_type: str):
        assert dataset_type == "match"

        competitions = self.get(data_spec_version="v4", path="competitions")

        def get_last_modified(competition):
            if not competition["match_updated"]:
                return None

            last_modified = datetime.fromisoformat(
                competition["match_updated"] + "+00:00"
            )
            if competition["match_updated_360"]:
                last_modified = max(
                    last_modified,
                    datetime.fromisoformat(competition["match_updated_360"] + "+00:00"),
                )
            return last_modified

        return [
            dict(
                competition_id=competition["competition_id"],
                season_id=competition["season_id"],
                # Passing the LastModified for an entire competition allows Ingestify to entirely skip
                # this Selector based on a datetime based check. Dataset comparison won't happen. When the
                # DataSpecVersion is changed, but LastModified isn't changed on the Source, new files ARE NOT ingested!
                _last_modified=get_last_modified(competition),
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

        match_data_spec_version = data_spec_versions.get_version("match")

        matches = self.get(
            path=f"competitions/{competition_id}/seasons/{season_id}/matches",
            data_spec_version=match_data_spec_version,
        )

        for match in matches:
            if match_id:
                if match["match_id"] != match_id:
                    continue

            last_modified = datetime.fromisoformat(match["last_updated"] + "+00:00")

            if match["collection_status"] == "Complete":
                if match["match_status"] == "available":
                    state = DatasetState.COMPLETE
                else:
                    # This could be "processing"
                    state = DatasetState.PARTIAL
            else:
                state = DatasetState.SCHEDULED

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
                data_spec_version=match_data_spec_version,
                json_content=match,
            )

            if state.is_complete:
                name += f" / {match['home_score']}-{match['away_score']}"

                for data_feed_key in ["lineups", "events"]:
                    for data_spec_version in data_spec_versions[data_feed_key]:
                        dataset_resource.add_file(
                            # Note: the LastModified value can be incorrect when only match Metadata (match file)
                            #       is changed. Use it anyway for indication. Ingestify will also use the
                            #       Dataset.last_modified_at value to determine if a file should be refetched
                            last_modified=last_modified,
                            data_feed_key=data_feed_key,
                            data_spec_version=data_spec_version,
                            url=self.get_url(
                                data_feed_key, data_spec_version, match["match_id"]
                            ),
                            http_options=dict(auth=(self.username, self.password)),
                            data_serialization_format="json",
                        )

                if (
                    match["last_updated_360"]
                    and match["match_status_360"] == "available"
                ):
                    for data_spec_version in data_spec_versions.get("360-frames", []):
                        dataset_resource.add_file(
                            last_modified=datetime.fromisoformat(
                                match["last_updated_360"] + "+00:00"
                            ),
                            data_feed_key="360-frames",
                            data_spec_version=data_spec_version,
                            url=self.get_url(
                                "360-frames", data_spec_version, match["match_id"]
                            ),
                            http_options=dict(auth=(self.username, self.password)),
                            data_serialization_format="json",
                        )

            yield dataset_resource
