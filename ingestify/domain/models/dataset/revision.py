from datetime import datetime
from enum import Enum
from typing import Dict, List

from typing_extensions import TypedDict

from .file import File
from ..base import BaseModel


class SourceType(str, Enum):
    TASK = "TASK"
    MANUAL = "MANUAL"


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
    # source: RevisionSource
    is_squashed: bool = False
    state: RevisionState = RevisionState.PENDING_VALIDATION

    @property
    def modified_files_map(self) -> Dict[str, File]:
        return {file.file_id: file for file in self.modified_files}

    def is_changed(self, files: Dict[str, datetime]) -> bool:
        modified_files_map = self.modified_files_map
        for file_id, last_modified in files.items():
            if file_id not in modified_files_map:
                return True

            if modified_files_map[file_id].modified_at < last_modified:
                return True

        return False
