from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

from typing_extensions import TypedDict

from .file import File
from ..base import BaseModel


class SourceType(str, Enum):
    TASK = "TASK"
    MANUAL = "MANUAL"
    SQUASHED = "SQUASHED"


class RevisionSource(TypedDict):
    source_type: SourceType
    source_id: str


class RevisionState(str, Enum):
    PENDING_VALIDATION = "PENDING_VALIDATION"
    VALIDATING = "VALIDATING"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class Revision(BaseModel):
    revision_id: int
    created_at: datetime
    description: str
    modified_files: List[File]
    source: Optional[RevisionSource]
    is_squashed: bool = False
    state: RevisionState = RevisionState.PENDING_VALIDATION

    @property
    def last_modified_at(self):
        return max(file.modified_at for file in self.modified_files)

    @property
    def modified_files_map(self) -> Dict[str, File]:
        return {file.file_id: file for file in self.modified_files}

    def is_changed(
        self, files: Dict[str, datetime], dataset_last_modified_at: datetime
    ) -> bool:
        modified_files_map = self.modified_files_map
        for file_id, last_modified in files.items():
            if file_id not in modified_files_map:
                return True

            if modified_files_map[file_id].modified_at < last_modified:
                if dataset_last_modified_at < last_modified:
                    # For StatsBomb we use last_modified of match for lineups, and events files.
                    # When only match is updated, the lineups and events files won't be updated
                    # as the content is not changed. Therefore, those modified_at is not updated,
                    # and we try to update it over and over again.
                    # This check prevents that; always take the LastModifiedAt of the Dataset
                    # into account
                    return True

        return False
