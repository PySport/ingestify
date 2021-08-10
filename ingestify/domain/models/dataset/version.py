from dataclasses import dataclass
from datetime import datetime
from typing import Union, Dict, List

from .file import DraftFile, File


@dataclass
class Version:
    version_id: int
    created_at: datetime
    description: str
    modified_files: List[Union[DraftFile, File]]
    is_squashed: bool = False

    @property
    def modified_files_map(self) -> Dict[str, File]:
        return {
            file.filename: file
            for file in self.modified_files
        }
