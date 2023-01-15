from dataclasses import dataclass
from datetime import datetime
from typing import BinaryIO


@dataclass
class DraftFile:
    modified_at: datetime
    tag: str
    size: int
    content_type: str

    stream: BinaryIO


@dataclass
class File:
    filename: str
    modified_at: datetime
    tag: str
    size: int
    content_type: str

    @classmethod
    def from_draft(cls, draft_file: DraftFile, filename: str) -> "File":
        return cls(
            filename=filename,
            modified_at=draft_file.modified_at,
            tag=draft_file.tag,
            size=draft_file.size,
            content_type=draft_file.content_type,
        )


@dataclass
class LoadedFile:
    filename: str
    modified_at: datetime
    tag: str
    size: int
    content_type: str
    stream: BinaryIO


__all__ = ["File", "DraftFile", "LoadedFile"]
