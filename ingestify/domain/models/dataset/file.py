from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import BinaryIO, Optional


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
    storage_size: int
    content_type: str

    path: Path

    # This can be used when a Version is squashed
    version_id: Optional[int] = None

    @classmethod
    def from_draft(
        cls, draft_file: DraftFile, filename: str, storage_size: int, path: Path
    ) -> "File":
        return cls(
            filename=filename,
            modified_at=draft_file.modified_at,
            tag=draft_file.tag,
            size=draft_file.size,
            storage_size=storage_size,
            content_type=draft_file.content_type,
            path=path,
        )


@dataclass
class LoadedFile:
    filename: str
    modified_at: datetime
    tag: str
    size: int
    storage_size: int
    content_type: str
    stream: BinaryIO
    path: Path

    # This can be used when a Version is squashed
    version_id: Optional[int] = None


__all__ = ["File", "DraftFile", "LoadedFile"]
