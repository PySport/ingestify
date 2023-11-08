import gzip
import hashlib
import mimetypes
import os
import shutil
from dataclasses import asdict
from io import BytesIO, StringIO

from typing import Dict, List, Optional, Union, Callable, BinaryIO

from ingestify.domain.models.dataset.events import VersionAdded, DatasetUpdated
from ingestify.domain.models.event import EventBus
from ingestify.domain.models import (
    Dataset,
    DatasetCollection,
    DatasetRepository,
    DraftFile,
    File,
    LoadedFile,
    FileRepository,
    Identifier,
    Selector,
    Version,
    DatasetCreated,
)
from ingestify.utils import utcnow, map_in_pool


class DatasetStore:
    def __init__(
        self,
        dataset_repository: DatasetRepository,
        file_repository: FileRepository,
        bucket: str,
    ):
        self.dataset_repository = dataset_repository
        self.file_repository = file_repository
        self.file_compression = "gzip"
        self.bucket = bucket
        self.event_bus: Optional[EventBus] = None

    # def __getstate__(self):
    #     return {"file_repository": self.file_repository, "bucket": self.bucket}

    def set_event_bus(self, event_bus: EventBus):
        self.event_bus = event_bus

    def dispatch(self, event):
        if self.event_bus:
            self.event_bus.dispatch(event)

    def get_dataset_collection(
        self,
        dataset_type: Optional[str] = None,
        provider: Optional[str] = None,
        dataset_id: Optional[str] = None,
        **selector,
    ) -> DatasetCollection:
        if "selector" in selector:
            selector = selector["selector"]
        if isinstance(selector, dict):
            selector = Selector(selector)

        dataset_collection = self.dataset_repository.get_dataset_collection(
            bucket=self.bucket,
            dataset_type=dataset_type,
            dataset_id=dataset_id,
            provider=provider,
            selector=selector,
        )
        dataset_collection.set_store(self)
        return dataset_collection

    #
    # def destroy_dataset(self, dataset_id: str):
    #     dataset = self.dataset_repository.
    #     self.dataset_repository.destroy_dataset(dataset_id)

    def _prepare_write_stream(self, file_: DraftFile) -> tuple[BytesIO, int, str]:
        if self.file_compression == "gzip":
            stream = BytesIO()
            with gzip.GzipFile(fileobj=stream, compresslevel=9, mode="wb") as fp:
                shutil.copyfileobj(file_.stream, fp)

            stream.seek(0, os.SEEK_END)
            storage_size = stream.tell()
            stream.seek(0)
            suffix = ".gz"
        else:
            stream = file_.stream
            storage_size = file_.size
            suffix = ""

        return stream, storage_size, suffix

    def _prepare_read_stream(self) -> tuple[Callable[[BinaryIO], BytesIO], str]:
        if self.file_compression == "gzip":

            def reader(fh: BinaryIO) -> BytesIO:
                stream = BytesIO()
                with gzip.GzipFile(fileobj=fh, compresslevel=9, mode="rb") as fp:
                    shutil.copyfileobj(fp, stream)
                stream.seek(0)
                return stream

            return reader, ".gz"
        else:
            return lambda fh: fh, ""

    def _persist_files(
        self,
        dataset: Dataset,
        version_id: int,
        modified_files: Dict[str, Optional[DraftFile]],
    ) -> List[File]:
        modified_files_ = []

        current_version = dataset.current_version

        for filename, file_ in modified_files.items():
            if isinstance(file_, (str, bytes, BytesIO, StringIO)):
                if isinstance(file_, str):
                    stream = BytesIO(file_.encode("utf-8"))
                elif isinstance(file_, bytes):
                    stream = BytesIO(file_)
                elif isinstance(file_, StringIO):
                    stream = BytesIO(file_.read().encode("utf-8"))
                elif isinstance(file_, BytesIO):
                    stream = file_
                else:
                    raise Exception("not possible")

                data = stream.read()
                size = len(data)
                tag = hashlib.sha1(data).hexdigest()
                stream.seek(0)

                if (
                    current_version
                    and (
                        current_file := current_version.modified_files_map.get(filename)
                    )
                    and current_file.tag == tag
                ):
                    file_ = None
                else:
                    file_ = DraftFile(
                        modified_at=utcnow(),
                        content_type=mimetypes.guess_type(filename)[0],
                        tag=tag,
                        size=size,
                        stream=stream,
                    )

            if isinstance(file_, DraftFile):
                stream, storage_size, suffix = self._prepare_write_stream(file_)

                # TODO: check if this is a very clean way to go from DraftFile to File
                full_path = self.file_repository.save_content(
                    bucket=self.bucket,
                    dataset=dataset,
                    version_id=version_id,
                    filename=filename + suffix,
                    stream=stream,
                )
                file = File.from_draft(
                    file_,
                    filename,
                    storage_size=storage_size,
                    path=self.file_repository.get_relative_path(full_path),
                )

                modified_files_.append(file)

        return modified_files_

    def add_version(
        self, dataset: Dataset, files: Dict[str, DraftFile], description: str = "Update"
    ):
        """
        Create new version first, so FileRepository can use
        version_id in the key.
        """
        version_id = dataset.next_version_id()
        created_at = utcnow()

        persisted_files_ = self._persist_files(dataset, version_id, files)
        dataset.add_version(
            Version(
                version_id=version_id,
                created_at=created_at,
                description=description,
                modified_files=persisted_files_,
            )
        )

        self.dataset_repository.save(bucket=self.bucket, dataset=dataset)
        self.dispatch(VersionAdded(dataset=dataset))

    def update_dataset(
        self,
        dataset: Dataset,
        dataset_identifier: Identifier,
        files: Dict[str, DraftFile],
    ):
        """The add_version will also save the dataset."""
        dataset_changed = False
        if dataset.update_from_identifier(dataset_identifier):
            self.dataset_repository.save(bucket=self.bucket, dataset=dataset)
            dataset_changed = True

        self.add_version(dataset, files)

        if dataset_changed:
            # Dispatch after version added. Otherwise the downstream handlers are not able to see
            # the new version
            self.dispatch(DatasetUpdated(dataset=dataset))

    def destroy_dataset(self, dataset: Dataset):
        # TODO: remove files. Now we leave some orphaned files around
        self.dataset_repository.destroy(dataset)

    def create_dataset(
        self,
        dataset_type: str,
        provider: str,
        dataset_identifier: Identifier,
        files: Dict[str, DraftFile],
        description: str = "Create",
    ):
        now = utcnow()

        dataset = Dataset(
            bucket=self.bucket,
            dataset_id=self.dataset_repository.next_identity(),
            name=dataset_identifier.name,
            state=dataset_identifier.state,
            identifier=dataset_identifier,
            dataset_type=dataset_type,
            provider=provider,
            metadata=dataset_identifier.metadata,
            created_at=now,
            updated_at=now,
        )
        self.add_version(dataset, files, description)

        self.dispatch(DatasetCreated(dataset=dataset))

    def load_files(self, dataset: Dataset) -> Dict[str, LoadedFile]:
        current_version = dataset.current_version
        files = {}

        reader, suffix = self._prepare_read_stream()
        for file in current_version.modified_files:
            # TODO: refactor

            loaded_file = LoadedFile(
                stream=reader(
                    self.file_repository.load_content(
                        bucket=self.bucket,
                        dataset=dataset,
                        # When file.version_id is set we must use it.
                        version_id=file.version_id or current_version.version_id,
                        filename=file.filename + suffix,
                    )
                ),
                **asdict(file),
            )
            files[file.filename] = loaded_file
        return files

    def load_with_kloppy(self, dataset: Dataset, **kwargs):
        files = self.load_files(dataset)
        if dataset.provider == "statsbomb":
            from kloppy import statsbomb

            try:
                return statsbomb.load(
                    event_data=files["events.json"].stream,
                    lineup_data=files["lineups.json"].stream,
                    **kwargs,
                )
            except Exception as e:
                raise Exception(f"Error loading {dataset}") from e
        elif dataset.provider == "wyscout":
            from kloppy import wyscout

            return wyscout.load(
                event_data=files["events.json"].stream, data_version="V3", **kwargs
            )
        else:
            raise Exception(f"Don't know how to load a '{dataset.provider}' dataset")

    # def load_content(self, dataset_id: str, version_id: int, filename: str):
    #     datasets = self.dataset_repository.get_dataset_collection(
    #         bucket=self.bucket, dataset_id=dataset_id
    #     )
    #     if not len(datasets):
    #         raise Exception("Not found")
    #     else:
    #         dataset = datasets.get_dataset_by_id(dataset_id)
    #
    #     return self.file_repository.load_content(
    #         bucket=self.bucket,
    #         dataset=dataset,
    #         version_id=version_id,
    #         filename=filename,
    #     )

    def map(
        self, fn, dataset_collection: DatasetCollection, processes: Optional[int] = None
    ):
        return map_in_pool(fn, dataset_collection, processes)
