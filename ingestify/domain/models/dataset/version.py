from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Union

from .file import File, FileNotModified, DraftFile


@dataclass
class DatasetVersion:
    created_at: datetime
    description: str
    files: Dict[str, Union[DraftFile, File, FileNotModified]]
    is_squashed: bool = False
