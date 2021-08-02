from dataclasses import dataclass
from datetime import datetime
from typing import IO, AnyStr


@dataclass
class DraftFile:
    modified_at: datetime
    tag: str
    size: int
    content_type: str

    stream: IO[AnyStr]


@dataclass
class File:
    file_id: str
    modified_at: datetime
    tag: str
    size: int
    content_type: str


@dataclass
class FileNotModified:
    pass


__all__ = ["File", "DraftFile", "FileNotModified"]
