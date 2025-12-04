from datetime import datetime
from typing import Optional, Callable, Any, Protocol, TYPE_CHECKING, Dict  # noqa
from pydantic import Field

from ingestify.domain.models.base import BaseModel
from ingestify.domain.models.dataset.dataset_state import DatasetState
from ingestify.exceptions import DuplicateFile

from ingestify.domain.models import File, DraftFile


class FileLoaderProtocol(Protocol):
    def __call__(
        self,
        file_resource: "FileResource",
        file: Optional["File"] = None,
        **kwargs: Any,
    ) -> Optional["DraftFile"]:
        ...


class FileResource(BaseModel):
    dataset_resource: "DatasetResource"
    file_id: str
    last_modified: datetime
    data_feed_key: str
    data_spec_version: str
    json_content: Optional[dict] = None
    url: Optional[str] = None
    http_options: Optional[dict] = None
    # DataSerializationFormat is "json" in case of json_content, otherwise file_loader will return it
    data_serialization_format: Optional[str] = None
    file_loader: Optional[
        Callable[["FileResource", Optional["File"]], Optional["DraftFile"]]
    ] = None
    loader_kwargs: dict = Field(default_factory=dict)

    def __post_init__(self):
        if self.json_content is None and not self.url and not self.file_loader:
            raise TypeError(
                "You need to specify `json_content`, `url` or a custom `file_loader`"
            )


class DatasetResource(BaseModel):
    dataset_resource_id: dict
    dataset_type: str
    provider: str
    name: str
    metadata: dict = Field(default_factory=dict)
    state: DatasetState = Field(default_factory=lambda: DatasetState.COMPLETE)
    files: dict[str, FileResource] = Field(default_factory=dict)
    post_load_files: Optional[
        Callable[["DatasetResource", Dict[str, DraftFile]], None]
    ] = None

    def run_post_load_files(self, files: Dict[str, DraftFile]):
        """Hook to modify dataset attributes based on loaded file content.

        Useful for setting state based on file content, e.g., keep state=SCHEDULED
        when files contain '{}', change to COMPLETE when they contain actual data.
        """
        if self.post_load_files:
            self.post_load_files(self, files)

    def add_file(
        self,
        last_modified: datetime,
        data_feed_key: str,
        # Some sources might not have a DataSpecVersion. Set a default
        data_spec_version: str = "v1",
        json_content: Optional[dict] = None,
        url: Optional[str] = None,
        http_options: Optional[dict] = None,
        data_serialization_format: Optional[str] = None,
        file_loader: Optional[
            Callable[
                [FileResource, Optional[File]],
                Optional[DraftFile],
            ]
        ] = None,
        loader_kwargs: Optional[dict] = None,
    ):
        file_id = f"{data_feed_key}__{data_spec_version}"
        if file_id in self.files:
            raise DuplicateFile(f"File with id {file_id} already exists.")

        file_resource = FileResource(
            dataset_resource=self,
            file_id=file_id,
            data_feed_key=data_feed_key,
            data_spec_version=data_spec_version,
            last_modified=last_modified,
            json_content=json_content,
            url=url,
            http_options=http_options,
            data_serialization_format=data_serialization_format,
            file_loader=file_loader,
            loader_kwargs=loader_kwargs or {},
        )

        self.files[file_id] = file_resource

        # Allow chaining
        return self
