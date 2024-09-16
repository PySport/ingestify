from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from ingestify.domain import DraftFile, File
    from ingestify.domain.models.dataset.dataset import DatasetState


@dataclass(frozen=True)
class FileResource:
    file_id: str
    last_modified: datetime
    data_feed_key: str
    data_spec_version: str

    # DataSerializationFormat is "json" in case of json_content, otherwise file_loader will return it
    # data_serialization_format: str

    json_content: Optional[str] = None
    file_loader: Optional[
        Callable[
            ["DatasetResource", "FileResource", Optional["File"]], Optional["DraftFile"]
        ]
    ] = None


class DatasetResource:
    def __init__(
        self, dataset_resource_id: dict, name: str, metadata: dict, state: "DatasetState"
    ):
        self.dataset_resource_id = dataset_resource_id
        self.name = name
        self.metadata = metadata
        self.state = state

        self._files = {}

    def add_file_resource(
        self,
        last_modified: datetime,
        data_feed_key: str,
        data_spec_version: str,
        json_content: Optional[str] = None,
        file_loader: Optional[
            Callable[
                ["DatasetResource", "FileResource", Optional["File"]], Optional["DraftFile"]
            ]
        ] = None,
    ):
        file_id = f"{data_feed_key}__{data_spec_version}"

        file_resource = FileResource(
            file_id=file_id,
            data_feed_key=data_feed_key,
            data_spec_version=data_spec_version,
            last_modified=last_modified,
            json_content=json_content,
            file_loader=file_loader,
        )

        self._files[file_id] = file_resource
