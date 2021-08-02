from dataclasses import dataclass
from typing import List, Optional

from .identifier import DatasetIdentifier
from .version import DatasetVersion
from .. import FileNotModified, DraftFile


@dataclass
class Dataset:
    identifier: DatasetIdentifier
    versions: List[DatasetVersion]

    @property
    def current_version(self) -> Optional[DatasetVersion]:
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
                for filename, file in version.files.items():
                    if isinstance(file, DraftFile):
                        raise Exception(
                            f"Cannot squash draft file. Version: {version}. Filename: {filename}"
                        )
                    if not isinstance(file, FileNotModified):
                        files[filename] = file

            return DatasetVersion(
                created_at=utcnow(),
                description="Squashed version",
                is_squashed=True,
                files=files,
            )
