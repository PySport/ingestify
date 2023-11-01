from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

from .file import File


@dataclass
class Version:
    version_id: int
    created_at: datetime
    description: str
    modified_files: List[File]
    is_squashed: bool = False

    @property
    def modified_files_map(self) -> Dict[str, File]:
        return {file.filename: file for file in self.modified_files}

    def is_changed(self, files: Dict[str, datetime]) -> bool:
        modified_files_map = self.modified_files_map
        for filename, last_modified in files.items():
            if filename not in modified_files_map:
                return True

            if modified_files_map[filename].modified_at < last_modified:
                return True

        return False
