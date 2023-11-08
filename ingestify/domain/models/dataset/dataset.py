from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional, TYPE_CHECKING

from ingestify.utils import utcnow

from .file import DraftFile
from .identifier import Identifier
from .version import Version

if TYPE_CHECKING:
    from ingestify.application.dataset_store import DatasetStore


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

    # current_version_id: int = 0
    versions: List[Version] = field(default_factory=list)

    store: Optional["DatasetStore"] = None

    def set_store(self, store):
        self.store = store

    def next_version_id(self):
        return len(self.versions)

    def add_version(self, version: Version):
        self.versions.append(version)
        self.updated_at = utcnow()

    def update_from_identifier(self, dataset_identifier: Identifier) -> bool:
        changed = False
        if self.name != dataset_identifier.name:
            self.name = dataset_identifier.name
            changed = True

        if self.metadata != dataset_identifier.metadata:
            self.metadata = dataset_identifier.metadata
            changed = True

        if self.state != dataset_identifier.state:
            self.state = dataset_identifier.state
            changed = True

        if changed:
            self.updated_at = utcnow()

        return changed

    @property
    def current_version(self) -> Optional[Version]:
        """
        When multiple versions are available, squash versions into one single version which
        contents all most recent files.
        """
        if not self.versions:
            return None
        elif len(self.versions) == 1:
            return self.versions[0]
        else:
            files = {}

            for version in self.versions:
                for filename, file in version.modified_files_map.items():
                    if isinstance(file, DraftFile):
                        raise Exception(
                            f"Cannot squash draft file. Version: {version}. Filename: {filename}"
                        )
                    files[filename] = file
                    files[filename].version_id = version.version_id

            return Version(
                version_id=self.versions[-1].version_id,
                created_at=self.versions[-1].created_at,
                # created_at=max([file.modified_at for file in files.values()]),
                description="Squashed version",
                is_squashed=True,
                modified_files=list(files.values()),
            )

    def to_kloppy(self):
        if not self.store:
            raise AttributeError("Store not set")
        return self.store.load_with_kloppy(self)
