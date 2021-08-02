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

    @classmethod
    def from_draft(cls, draft_file: DraftFile, file_id: str) -> 'File':
        return cls(
            file_id=file_id,
            modified_at=draft_file.modified_at,
            tag=draft_file.tag,
            size=draft_file.size,
            content_type=draft_file.content_type
        )


@dataclass
class FileNotModified:
    pass


__all__ = ["File", "DraftFile", "FileNotModified"]
