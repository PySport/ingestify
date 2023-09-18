from dataclasses import dataclass, field
from typing import List, Optional, TYPE_CHECKING

from ingestify.utils import utcnow

from .file import DraftFile
from .identifier import Identifier
from .version import Version

if TYPE_CHECKING:
    from ingestify.application.dataset_store import DatasetStore


@dataclass
class Dataset:
    bucket: str  # This must be set by the DatasetRepository

    dataset_id: str

    dataset_type: str
    provider: str

    identifier: Identifier
    metadata: dict

    # current_version_id: int = 0
    versions: List[Version] = field(default_factory=list)

    store: Optional["DatasetStore"] = None

    def set_store(self, store):
        self.store = store

    def next_version_id(self):
        return len(self.versions)

    def add_version(self, version: Version):
        self.versions.append(version)

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

            return Version(
                version_id=self.versions[-1].version_id,
                created_at=max([file.modified_at for file in files.values()]),
                description="Squashed version",
                is_squashed=True,
                modified_files=list(files.values()),
            )

    def to_kloppy(self):
        if not self.store:
            raise AttributeError("Store not set")
        return self.store.load_with_kloppy(self)
