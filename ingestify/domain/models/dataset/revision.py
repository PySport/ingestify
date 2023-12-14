from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

from .file import File


@dataclass
class Revision:
    revision_id: int
    created_at: datetime
    description: str
    modified_files: List[File]
    is_squashed: bool = False

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
