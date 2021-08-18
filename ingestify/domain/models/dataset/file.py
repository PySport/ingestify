from dataclasses import dataclass
from datetime import datetime
from typing import IO, AnyStr, BinaryIO


@dataclass
class DraftFile:
    modified_at: datetime
    tag: str
    size: int
    content_type: str

    stream: IO[AnyStr]


@dataclass
class File:
    filename: str
    file_key: str
    modified_at: datetime
    tag: str
    size: int
    content_type: str

    @classmethod
    def from_draft(cls, draft_file: DraftFile, file_key: str, filename: str) -> "File":
        return cls(
            file_key=file_key,
            filename=filename,
            modified_at=draft_file.modified_at,
            tag=draft_file.tag,
            size=draft_file.size,
            content_type=draft_file.content_type,
        )


@dataclass
class LoadedFile(File):
    stream: BinaryIO


__all__ = ["File", "DraftFile", "LoadedFile"]
