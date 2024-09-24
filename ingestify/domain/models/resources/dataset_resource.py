from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Callable, TYPE_CHECKING

from ingestify.exceptions import DuplicateFile

if TYPE_CHECKING:
    from ingestify.domain import DraftFile, File
    from ingestify.domain.models.dataset.dataset import DatasetState


@dataclass(frozen=True)
class FileResource:
    dataset_resource: "DatasetResource"
    file_id: str
    last_modified: datetime
    data_feed_key: str
    data_spec_version: str

    # DataSerializationFormat is "json" in case of json_content, otherwise file_loader will return it
    # data_serialization_format: str

    json_content: Optional[dict] = None

    url: Optional[str] = None
    http_options: Optional[dict] = None
    data_serialization_format: Optional[str] = None

    file_loader: Optional[
        Callable[["FileResource", Optional["File"]], Optional["DraftFile"]]
    ] = None

    def __post_init__(self):
        if self.json_content is None and not self.url and not self.file_loader:
            raise TypeError(
                "You need to specify `json_content`, `url` or a custom `file_loader`"
            )


class DatasetResource:
    def __init__(
        self,
        dataset_resource_id: dict,
        /,
        dataset_type: str,
        provider: str,
        name: str,
        metadata: Optional[dict] = None,
        state: Optional["DatasetState"] = None,
    ):
        from ingestify.domain.models.dataset.dataset import DatasetState

        self.dataset_type = dataset_type
        self.provider = provider
        self.dataset_resource_id = dataset_resource_id
        self.name = name
        self.metadata = metadata or {}
        self.state = state or DatasetState.COMPLETE

        self.files = {}

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
                ["FileResource", Optional["File"]],
                Optional["DraftFile"],
            ]
        ] = None,
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
        )

        self.files[file_id] = file_resource

        # Allow chaining
        return self
