from datetime import datetime
from pathlib import Path
from typing import BinaryIO, Optional, Union, Callable, Awaitable
from io import BytesIO, StringIO
import hashlib

from ingestify.domain.models.base import BaseModel
from ingestify.utils import utcnow


class DraftFile(BaseModel):
    created_at: datetime
    modified_at: datetime
    tag: str
    size: int
    content_type: Optional[str]
    data_feed_key: str  # Example: 'events'
    data_spec_version: str  # Example: 'v3'
    data_serialization_format: str  # Example: 'json'
    stream: BytesIO

    @classmethod
    def from_input(
        cls,
        file_,
        data_feed_key: str,
        data_spec_version: str = "v1",
        data_serialization_format: str = "txt",
        modified_at: Optional[datetime] = None,
    ):
        # Pass-through for these types
        if isinstance(file_, (DraftFile, NotModifiedFile)):
            return file_
        elif isinstance(file_, str):
            stream = BytesIO(file_.encode("utf-8"))
        elif isinstance(file_, bytes):
            stream = BytesIO(file_)
        elif isinstance(file_, StringIO):
            stream = BytesIO(file_.read().encode("utf-8"))
        elif isinstance(file_, BytesIO):
            stream = file_
        elif hasattr(file_, "read"):
            data = file_.read()
            if isinstance(data, bytes):
                stream = BytesIO(data)
            else:
                stream = BytesIO(data)
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


class File(BaseModel):
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
    revision_id: Optional[int] = None  # This can be used when a Version is squashed

    @classmethod
    def from_draft(
        cls,
        draft_file: DraftFile,
        file_id: str,
        storage_size: int,
        storage_compression_method: str,
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


class NotModifiedFile(BaseModel):
    modified_at: datetime
    reason: str


class LoadedFile(BaseModel):
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
    data_serialization_format: Optional[str]  # Example: 'json'
    storage_compression_method: Optional[str]  # Example: 'gzip'
    storage_path: Path
    stream_: Union[BinaryIO, BytesIO, Callable[[], Awaitable[Union[BinaryIO, BytesIO]]]]
    revision_id: Optional[int] = None  # This can be used when a Revision is squashed

    def load_stream(self):
        if callable(self.stream_):
            self.stream_ = self.stream_(self)

    @property
    def stream(self):
        if callable(self.stream_):
            raise Exception("You should load the stream first using `load_stream`")
        return self.stream_


__all__ = ["File", "DraftFile", "LoadedFile", "NotModifiedFile"]
