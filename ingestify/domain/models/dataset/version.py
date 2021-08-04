from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Union

from .file import DraftFile, File, FileNotModified


@dataclass
class DatasetVersion:
    version_id: int
    created_at: datetime
    description: str
    files: Dict[str, Union[DraftFile, File, FileNotModified]]
    is_squashed: bool = False
