from datetime import datetime
from typing import Optional, Dict, Union, List, Iterator

import pytz

from ingestify import Source
from ingestify.domain import Identifier, DataSpecVersionCollection, Revision, DraftFile


class FakeSource(Source):
    @property
    def provider(self) -> str:
        return "fake"

    def discover_datasets(
        self,
        dataset_type: str,
        data_spec_versions: DataSpecVersionCollection,
        competition_id,
        season_id,
        **kwargs
    ) -> Union[List[Dict], Iterator[List[Dict]]]:
        return [
            dict(
                competition_id=competition_id,
                season_id=season_id,
                _name="Test Dataset",
                _last_modified=datetime.now(pytz.utc),
            )
        ]

    def fetch_dataset_files(
        self,
        dataset_type: str,
        identifier: Identifier,
        data_spec_versions: DataSpecVersionCollection,
        current_revision: Optional[Revision],
    ) -> Dict[str, Optional[DraftFile]]:
        if current_revision:
            return {
                "file1": DraftFile.from_input(
                    "different_content",
                ),
                "file2": DraftFile.from_input("some_content" + identifier.key),
            }
        else:
            return {
                "file1": DraftFile.from_input(
                    "content1",
                ),
                "file2": DraftFile.from_input("some_content" + identifier.key),
            }
