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
