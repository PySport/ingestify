from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict
from pydantic import Field, field_validator

from ingestify.utils import utcnow
from .dataset_state import DatasetState
from .file import DraftFile
from .identifier import Identifier
from .revision import Revision, RevisionSource, SourceType
from ..base import BaseModel


class Dataset(BaseModel):
    bucket: str  # This must be set by the DatasetRepository
    dataset_id: str
    name: str
    state: DatasetState
    dataset_type: str
    provider: str
    identifier: Identifier
    metadata: dict
    created_at: datetime
    updated_at: datetime

    revisions: List[Revision] = Field(default_factory=list)
    # The last_modified_at is equal to the max modified_at of all files in all revisions
    last_modified_at: Optional[datetime]

    @field_validator("identifier", mode="before")
    @classmethod
    def parse_identifier(cls, value):
        if not isinstance(value, Identifier):
            return Identifier(value)
        return value

    @property
    def is_complete(self):
        return self.state.is_complete

    def next_revision_id(self) -> int:
        return len(self.revisions)

    def add_revision(self, revision: Revision):
        self.revisions.append(revision)
        self.updated_at = utcnow()

        if self.last_modified_at:
            self.last_modified_at = max(
                self.last_modified_at, revision.last_modified_at
            )
        else:
            self.last_modified_at = revision.last_modified_at

    def update_last_modified(self, files: Dict[str, DraftFile]):
        """Update the last modified, even tho there was no new revision. Some Sources
        may report a Dataset is changed, even when there are no changed files.
        Update the last_modified to prevent hitting the same Source for updates
        """
        changed = False
        for file in files.values():
            if file.modified_at and (
                self.last_modified_at is None
                or file.modified_at > self.last_modified_at
            ):
                # Update, and continue looking for others
                self.last_modified_at = file.modified_at
                changed = True
        return changed

    def update_metadata(self, name: str, metadata: dict, state: DatasetState) -> bool:
        changed = False
        if self.name != name:
            self.name = name
            changed = True

        if self.metadata != metadata:
            self.metadata = metadata
            changed = True

        if self.state != state:
            self.state = state
            changed = True

        if changed:
            self.updated_at = utcnow()

        return changed

    @property
    def current_revision(self) -> Optional[Revision]:
        """
        When multiple versions are available, squash versions into one single version which
        contents all most recent files.
        """
        if not self.revisions:
            return None
        elif len(self.revisions) == 1:
            return self.revisions[0]
        else:
            files = {}

            for revision in self.revisions:
                for file_id, file in revision.modified_files_map.items():
                    if isinstance(file, DraftFile):
                        raise Exception(
                            f"Cannot squash draft file. Revision: {revision}. FileId: {file_id}"
                        )
                    files[file_id] = file
                    files[file_id].revision_id = revision.revision_id

            return Revision(
                revision_id=self.revisions[-1].revision_id,
                created_at=self.revisions[-1].created_at,
                # created_at=max([file.modified_at for file in files.values()]),
                description="Squashed revision",
                is_squashed=True,
                modified_files=list(files.values()),
                source=RevisionSource(source_type=SourceType.SQUASHED, source_id=""),
            )
