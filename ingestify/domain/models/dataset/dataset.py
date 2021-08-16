from dataclasses import dataclass, field
from typing import List, Optional

from ingestify.utils import utcnow

from .file import DraftFile
from .identifier import Identifier
from .version import Version


@dataclass
class Dataset:
    dataset_id: str

    dataset_type: str
    provider: str

    identifier: Identifier

    current_version_id: int = 0
    versions: List[Version] = field(default_factory=list)

    def next_version_id(self):
        version_id = self.current_version_id
        self.current_version_id += 1
        return version_id

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
                created_at=utcnow(),
                description="Squashed version",
                is_squashed=True,
                files=files,
            )
