import gzip
import hashlib
import logging
import mimetypes
import os
import shutil
from dataclasses import asdict
from io import BytesIO, StringIO

from typing import Dict, List, Optional, Union, Callable, BinaryIO

from ingestify.domain.models.dataset.dataset import DatasetState
from ingestify.domain.models.dataset.events import RevisionAdded, MetadataUpdated
from ingestify.domain.models.dataset.file_collection import FileCollection
from ingestify.domain.models.event import EventBus
from ingestify.domain.models import (
    Dataset,
    DatasetCollection,
    DatasetRepository,
    DatasetResource,
    DraftFile,
    File,
    LoadedFile,
    FileRepository,
    Identifier,
    Selector,
    Revision,
    DatasetCreated,
)
from ingestify.utils import utcnow, map_in_pool


logger = logging.getLogger(__name__)


class DatasetStore:
    def __init__(
        self,
        dataset_repository: DatasetRepository,
        file_repository: FileRepository,
        bucket: str,
    ):
        self.dataset_repository = dataset_repository
        self.file_repository = file_repository
        self.storage_compression_method = "gzip"
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
            # By-pass the build as we don't want to specify data_spec_versions here... (for now)
            selector = Selector(selector)
        elif isinstance(selector, list):
            if not selector:
                return DatasetCollection()

            if isinstance(selector[0], dict):
                # Convert all selector dicts to Selectors
                selector = [Selector(_) for _ in selector]

        dataset_collection = self.dataset_repository.get_dataset_collection(
            bucket=self.bucket,
            dataset_type=dataset_type,
            dataset_id=dataset_id,
            provider=provider,
            selector=selector,
        )
        return dataset_collection

    #
    # def destroy_dataset(self, dataset_id: str):
    #     dataset = self.dataset_repository.
    #     self.dataset_repository.destroy_dataset(dataset_id)

    def _prepare_write_stream(self, file_: DraftFile) -> tuple[BytesIO, int, str]:
        if self.storage_compression_method == "gzip":
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
        if self.storage_compression_method == "gzip":

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
        revision_id: int,
        modified_files: Dict[str, Optional[DraftFile]],
    ) -> List[File]:
        modified_files_ = []

        current_revision = dataset.current_revision

        for file_id, file_ in modified_files.items():
            if file_ is None:
                # It's always allowed to pass None as file. This means it didn't change and must be ignored.
                continue

            current_file = (
                current_revision.modified_files_map.get(file_id)
                if current_revision
                else None
            )
            if current_file and current_file.tag == file_.tag:
                # File didn't change. Ignore it.
                continue

            stream, storage_size, suffix = self._prepare_write_stream(file_)

            # TODO: check if this is a very clean way to go from DraftFile to File
            full_path = self.file_repository.save_content(
                bucket=self.bucket,
                dataset=dataset,
                revision_id=revision_id,
                filename=file_id + "." + file_.data_serialization_format + suffix,
                stream=stream,
            )
            file = File.from_draft(
                file_,
                file_id,
                storage_size=storage_size,
                storage_compression_method=self.storage_compression_method,
                path=self.file_repository.get_relative_path(full_path),
            )

            modified_files_.append(file)

        return modified_files_

    def add_revision(
        self, dataset: Dataset, files: Dict[str, DraftFile], description: str = "Update"
    ):
        """
        Create new revision first, so FileRepository can use
        revision_id in the key.
        """
        revision_id = dataset.next_revision_id()
        created_at = utcnow()

        persisted_files_ = self._persist_files(dataset, revision_id, files)
        if persisted_files_:
            # It can happen an API tells us data is changed, but it was not changed. In this case
            # we decide to ignore it.
            # Make sure there are files changed before creating a new revision
            dataset.add_revision(
                Revision(
                    revision_id=revision_id,
                    created_at=created_at,
                    description=description,
                    modified_files=persisted_files_,
                )
            )

            self.dataset_repository.save(bucket=self.bucket, dataset=dataset)
            self.dispatch(RevisionAdded(dataset=dataset))
            logger.info(
                f"Added a new revision to {dataset.identifier} -> {', '.join([file.file_id for file in persisted_files_])}"
            )
            return True
        else:
            logger.info(
                f"Ignoring a new revision without changed files -> {dataset.identifier}"
            )
            return False

    def update_dataset(
        self,
        dataset: Dataset,
        dataset_resource: DatasetResource,
        files: Dict[str, DraftFile],
    ):
        """The add_revision will also save the dataset."""
        metadata_changed = False
        if dataset.update_from_resource(dataset_resource):
            self.dataset_repository.save(bucket=self.bucket, dataset=dataset)
            metadata_changed = True

        self.add_revision(dataset, files)

        if metadata_changed:
            # Dispatch after revision added. Otherwise, the downstream handlers are not able to see
            # the new revision
            self.dispatch(MetadataUpdated(dataset=dataset))

    def destroy_dataset(self, dataset: Dataset):
        # TODO: remove files. Now we leave some orphaned files around
        self.dataset_repository.destroy(dataset)

    def create_dataset(
        self,
        dataset_type: str,
        provider: str,
        dataset_identifier: Identifier,
        name: str,
        state: DatasetState,
        metadata: dict,
        files: Dict[str, DraftFile],
        description: str = "Create",
    ):
        now = utcnow()

        dataset = Dataset(
            bucket=self.bucket,
            dataset_id=self.dataset_repository.next_identity(),
            name=name,
            state=state,
            identifier=dataset_identifier,
            dataset_type=dataset_type,
            provider=provider,
            metadata=metadata,
            created_at=now,
            updated_at=now,
        )
        self.add_revision(dataset, files, description)

        self.dispatch(DatasetCreated(dataset=dataset))

    def load_files(
        self,
        dataset: Dataset,
        data_feed_keys: Optional[List[str]] = None,
        lazy: bool = False,
        auto_rewind: bool = True,
    ) -> FileCollection:
        current_revision = dataset.current_revision
        files = {}

        reader, suffix = self._prepare_read_stream()
        for file in current_revision.modified_files:
            if data_feed_keys and file.data_feed_key not in data_feed_keys:
                continue

            def get_stream(file_):
                revision_id = file_.revision_id
                if revision_id is None:
                    revision_id = current_revision.revision_id

                return reader(
                    self.file_repository.load_content(
                        bucket=self.bucket,
                        dataset=dataset,
                        # When file.revision_id is set we must use it.
                        revision_id=revision_id,
                        filename=file_.file_id
                        + "."
                        + file_.data_serialization_format
                        + suffix,
                    )
                )

            loaded_file = LoadedFile(
                _stream=get_stream if lazy else get_stream(file),
                **asdict(file),
            )
            files[file.file_id] = loaded_file
        return FileCollection(files, auto_rewind=auto_rewind)

    def load_with_kloppy(self, dataset: Dataset, **kwargs):
        files = self.load_files(dataset)
        if dataset.provider == "statsbomb":
            from kloppy import statsbomb

            try:
                return statsbomb.load(
                    event_data=files.get_file("events").stream,
                    lineup_data=files.get_file("lineups").stream,
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
