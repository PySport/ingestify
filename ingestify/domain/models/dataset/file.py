import hashlib
import mimetypes

from dataclasses import dataclass
from datetime import datetime
from io import BytesIO, StringIO
from pathlib import Path
from typing import BinaryIO, Optional, Union, Callable

from ingestify.utils import utcnow


@dataclass
class DraftFile:
    created_at: datetime
    modified_at: datetime
    tag: str
    size: int
    content_type: Optional[str]

    data_feed_key: str  # Example: 'events'
    data_spec_version: str  # Example: 'v3'
    data_serialization_format: str  # Example: 'json'

    stream: BinaryIO

    @classmethod
    def from_input(
        cls,
        file_,
        data_feed_key,
        data_spec_version="v1",
        data_serialization_format="txt",
        modified_at=None,
    ):
        # Pass-through for these types
        if isinstance(file_, DraftFile) or file_ is None:
            return file_
        elif isinstance(file_, str):
            stream = BytesIO(file_.encode("utf-8"))
        elif isinstance(file_, bytes):
            stream = BytesIO(file_)
        elif isinstance(file_, StringIO):
            stream = BytesIO(file_.read().encode("utf-8"))
        elif isinstance(file_, BytesIO):
            stream = file_
        else:
            raise Exception(f"Not possible to create DraftFile from {type(file_)}")

        data = stream.read()
        size = len(data)
        tag = hashlib.sha1(data).hexdigest()
        stream.seek(0)

        now = utcnow()

        return DraftFile(
            created_at=now,
            modified_at=modified_at or now,
            tag=tag,
            size=size,
            stream=stream,
            content_type=None,
            data_feed_key=data_feed_key,
            data_spec_version=data_spec_version,
            data_serialization_format=data_serialization_format,
        )


@dataclass
class File:
    file_id: str
    created_at: datetime
    modified_at: datetime
    tag: str
    size: int
    content_type: Optional[str]

    data_feed_key: str  # Example: 'events'
    data_spec_version: str  # Example: 'v3'
    data_serialization_format: str  # Example: 'json'

    storage_size: int
    storage_compression_method: Optional[str]  # Example: 'gzip'
    storage_path: Path

    # This can be used when a Version is squashed
    revision_id: Optional[int] = None

    @classmethod
    def from_draft(
        cls,
        draft_file: DraftFile,
        file_id: str,
        storage_size: int,
        storage_compression_method,
        path: Path,
    ) -> "File":
        return cls(
            file_id=file_id,
            created_at=draft_file.created_at,
            modified_at=draft_file.modified_at,
            tag=draft_file.tag,
            size=draft_file.size,
            data_feed_key=draft_file.data_feed_key,
            data_spec_version=draft_file.data_spec_version,
            data_serialization_format=draft_file.data_serialization_format,
            content_type=draft_file.content_type,
            storage_size=storage_size,
            storage_compression_method=storage_compression_method,
            storage_path=path,
        )


@dataclass
class LoadedFile:
    # Unique key to identify this File within a Dataset
    file_id: str
    created_at: datetime
    modified_at: datetime
    tag: str
    size: int
    storage_size: int
    content_type: Optional[str]

    data_feed_key: str  # Example: 'events'
    data_spec_version: str  # Example: 'v3'
    data_serialization_format: Optional[str]  # Example: 'gzip'

    storage_size: int
    storage_compression_method: Optional[str]  # Example: 'gzip'
    storage_path: Path

    _stream: Union[BinaryIO, Callable[[], BinaryIO]]

    # This can be used when a Revision is squashed
    revision_id: Optional[int] = None

    @property
    def stream(self):
        if callable(self._stream):
            self._stream = self._stream(self)
        return self._stream


__all__ = ["File", "DraftFile", "LoadedFile"]
