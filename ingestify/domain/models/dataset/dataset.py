from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional

from ingestify.utils import utcnow

from .file import DraftFile
from .identifier import Identifier
from .revision import Revision


class DatasetState(Enum):
    SCHEDULED = "SCHEDULED"
    PARTIAL = "PARTIAL"
    COMPLETE = "COMPLETE"

    @property
    def is_complete(self):
        return self == DatasetState.COMPLETE

    def __str__(self):
        return self.value


@dataclass
class Dataset:
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

    revisions: List[Revision] = field(default_factory=list)

    @property
    def is_complete(self):
        return self.state.is_complete

    def next_revision_id(self):
        return len(self.revisions)

    def add_revision(self, revision: Revision):
        self.revisions.append(revision)
        self.updated_at = utcnow()

    def update_from_resource(self, dataset_resource) -> bool:
        changed = False
        if self.name != dataset_resource.name:
            self.name = dataset_resource.name
            changed = True

        if self.metadata != dataset_resource.metadata:
            self.metadata = dataset_resource.metadata
            changed = True

        if self.state != dataset_resource.state:
            self.state = dataset_resource.state
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
            )
