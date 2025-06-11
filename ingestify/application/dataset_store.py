import gzip
import logging
import os
import shutil
from contextlib import contextmanager
import threading
from io import BytesIO

from typing import (
    Dict,
    List,
    Optional,
    Union,
    Callable,
    BinaryIO,
    Awaitable,
    NewType,
    Iterable,
)

from ingestify.domain.models.dataset.dataset import DatasetState
from ingestify.domain.models.dataset.events import RevisionAdded, MetadataUpdated
from ingestify.domain.models.dataset.file import NotModifiedFile
from ingestify.domain.models.dataset.file_collection import FileCollection
from ingestify.domain.models.dataset.revision import RevisionSource
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
    Revision,
    DatasetCreated,
)
from ingestify.utils import utcnow


logger = logging.getLogger(__name__)

# Type definition for dataset state parameters that can be strings or DatasetState objects
DatasetStateParam = NewType(
    "DatasetStateParam", Union[str, Iterable[str], DatasetState, Iterable[DatasetState]]
)


def normalize_dataset_state(
    dataset_state: Optional[DatasetStateParam],
) -> Optional[List[DatasetState]]:
    """
    Normalize dataset_state parameter to a list of DatasetState objects.

    Args:
        dataset_state: Can be None, a string, a DatasetState enum,
                      or a list of strings or DatasetState enums

    Returns:
        None if input is None, otherwise a list of DatasetState objects

    Raises:
        ValueError: If an invalid state value is provided
        TypeError: If dataset_state contains elements of invalid types
        Warning: If an empty list is provided
    """
    if dataset_state is None:
        return None

    # Check for empty list
    if isinstance(dataset_state, list) and len(dataset_state) == 0:
        logger.warning(
            "Empty list provided for dataset_state, this will not filter any states"
        )
        return None

    normalized_states = []
    states_to_process = (
        [dataset_state] if not isinstance(dataset_state, list) else dataset_state
    )

    for state in states_to_process:
        if isinstance(state, str):
            # Handle case-insensitive string matching
            try:
                # Try to match the string to a DatasetState enum value
                normalized_state = DatasetState(state.upper())
                normalized_states.append(normalized_state)
            except ValueError:
                valid_states = ", ".join([s.value for s in DatasetState])
                raise ValueError(
                    f"Invalid dataset state: '{state}'. Valid states are: {valid_states}"
                )
        elif isinstance(state, DatasetState):
            # Already a DatasetState enum, just add it
            normalized_states.append(state)
        else:
            raise TypeError(
                f"Dataset state must be a string or DatasetState enum, got {type(state).__name__}"
            )

    return normalized_states


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
        # Create thread-local storage for caching
        self._thread_local = threading.local()

        # Pass current version to repository for validation/migration
        from ingestify import __version__

        self.dataset_repository.ensure_compatible_version(__version__)

    # def __getstate__(self):
    #     return {"file_repository": self.file_repository, "bucket": self.bucket}

    def set_event_bus(self, event_bus: EventBus):
        self.event_bus = event_bus

    def dispatch(self, event):
        if self.event_bus:
            self.event_bus.dispatch(event)

    @contextmanager
    def with_file_cache(self):
        """Context manager to enable file caching during its scope.

        Files loaded within this context will be cached and reused,
        avoiding multiple downloads of the same file.

        Example:
            # Without caching (loads files twice)
            analyzer1 = StatsAnalyzer(store, dataset)
            analyzer2 = VisualizationTool(store, dataset)

            # With caching (files are loaded once and shared)
            with store.with_file_cache():
                analyzer1 = StatsAnalyzer(store, dataset)
                analyzer2 = VisualizationTool(store, dataset)
        """
        # Enable caching for this thread
        self._thread_local.use_file_cache = True
        self._thread_local.file_cache = {}

        try:
            yield
        finally:
            # Disable caching for this thread
            self._thread_local.use_file_cache = False
            self._thread_local.file_cache = {}

    def save_ingestion_job_summary(self, ingestion_job_summary):
        self.dataset_repository.save_ingestion_job_summary(ingestion_job_summary)

    def get_dataset_collection(
        self,
        dataset_type: Optional[str] = None,
        provider: Optional[str] = None,
        dataset_id: Optional[str] = None,
        metadata_only: Optional[bool] = False,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
        dataset_state: Optional[DatasetStateParam] = None,
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

        # Normalize dataset_state to a list of DatasetState objects
        normalized_dataset_state = normalize_dataset_state(dataset_state)

        dataset_collection = self.dataset_repository.get_dataset_collection(
            bucket=self.bucket,
            dataset_type=dataset_type,
            dataset_id=dataset_id,
            provider=provider,
            metadata_only=metadata_only,
            selector=selector,
            dataset_state=normalized_dataset_state,
            page=page,
            page_size=page_size,
        )
        return dataset_collection

    def iter_dataset_collection_batches(
        self,
        dataset_type: Optional[str] = None,
        provider: Optional[str] = None,
        dataset_id: Optional[str] = None,
        batch_size: int = 1000,
        yield_dataset_collection: bool = False,
        dataset_state: Optional[DatasetStateParam] = None,
        **selector,
    ):
        """
        Iterate through all datasets matching the criteria with automatic pagination.

        Examples:
        ```
        # Iterate through individual datasets
        for dataset in store.iter_dataset_collection_batches(dataset_type="match", provider="statsbomb"):
            process(dataset)

        # Iterate through DatasetCollection objects (pages)
        for collection in store.iter_dataset_collection(
            dataset_type="match",
            provider="statsbomb",
            yield_dataset_collection=True
        ):
            process_collection(collection)

        # Filter by dataset state
        for dataset in store.iter_dataset_collection(
            dataset_type="match",
            dataset_state="COMPLETE"  # Can also use DatasetState.COMPLETE or ["COMPLETE", "PARTIAL"]
        ):
            process_completed_dataset(dataset)
        ```

        Args:
            dataset_type: Optional dataset type filter
            provider: Optional provider filter
            dataset_id: Optional dataset ID filter
            batch_size: Number of datasets to fetch per batch
            yield_dataset_collection: If True, yields entire DatasetCollection objects
                                     instead of individual Dataset objects
            dataset_state: Optional filter for dataset state. Can be a string, DatasetState enum,
                          or a list of strings or DatasetState enums
            **selector: Additional selector criteria

        Yields:
            If yield_dataset_collection is False (default): Dataset objects one by one
            If yield_dataset_collection is True: DatasetCollection objects (pages)
        """
        page = 1
        while True:
            collection = self.get_dataset_collection(
                dataset_type=dataset_type,
                provider=provider,
                dataset_id=dataset_id,
                page=page,
                page_size=batch_size,
                dataset_state=dataset_state,
                **selector,
            )

            if not collection or len(collection) == 0:
                break

            if yield_dataset_collection:
                yield collection
            else:
                for dataset in collection:
                    yield dataset

            # If we got fewer results than page_size, we've reached the end
            if len(collection) < batch_size:
                break

            page += 1

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

    def _prepare_read_stream(
        self,
    ) -> tuple[Callable[[BinaryIO], Awaitable[BytesIO]], str]:
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
            if isinstance(file_, NotModifiedFile):
                # It's always allowed to pass NotModifiedFile as file. This means it didn't change and must be ignored.
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
        self,
        dataset: Dataset,
        files: Dict[str, DraftFile],
        revision_source: RevisionSource,
        description: str = "Update",
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
            revision = Revision(
                revision_id=revision_id,
                created_at=created_at,
                description=description,
                modified_files=persisted_files_,
                source=revision_source,
            )

            dataset.add_revision(revision)

            self.dataset_repository.save(bucket=self.bucket, dataset=dataset)
            self.dispatch(RevisionAdded(dataset=dataset))
            logger.info(
                f"Added a new revision to {dataset.identifier} -> {', '.join([file.file_id for file in persisted_files_])}"
            )
        else:
            if dataset.update_last_modified(files):
                # For some Datasets the last modified doesn't make sense (for sources that don't provide it)
                # Do we want to update last modified of a Dataset when the value is utcnow()?
                # self.dataset_repository.save(bucket=self.bucket, dataset=dataset)
                # TODO: dispatch some event?
                # self.dispatch(DatasetLastModifiedChanged(dataset=dataset))
                logger.info(
                    f"Ignoring a new revision without changed files -> {dataset.identifier}, but "
                    f"might need to update last modified to {dataset.last_modified_at} ?"
                )

            else:
                logger.info(
                    f"Ignoring a new revision without changed files -> {dataset.identifier}"
                )

            revision = None

        return revision

    def update_dataset(
        self,
        dataset: Dataset,
        name: str,
        state: DatasetState,
        metadata: dict,
        files: Dict[str, DraftFile],
        revision_source: RevisionSource,
    ):
        """The add_revision will also save the dataset."""
        metadata_changed = False
        if dataset.update_metadata(name, metadata, state):
            self.dataset_repository.save(bucket=self.bucket, dataset=dataset)
            metadata_changed = True

        revision = self.add_revision(dataset, files, revision_source)

        if metadata_changed:
            # Dispatch after revision added. Otherwise, the downstream handlers are not able to see
            # the new revision
            self.dispatch(MetadataUpdated(dataset=dataset))

        return revision

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
        revision_source: RevisionSource,
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
            last_modified_at=None,  # Not known at this moment
        )
        revision = self.add_revision(dataset, files, revision_source, description)

        self.dispatch(DatasetCreated(dataset=dataset))
        return revision

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
                return reader(
                    self.file_repository.load_content(storage_path=file_.storage_path)
                )

            def make_loaded_file():
                return LoadedFile(
                    stream_=get_stream if lazy else get_stream(file),
                    **file.model_dump(),
                )

            # Using getattr with a default value of False - simple one-liner
            if getattr(self._thread_local, "use_file_cache", False):
                key = (dataset.dataset_id, current_revision.revision_id, file.file_id)
                if key not in self._thread_local.file_cache:
                    self._thread_local.file_cache[key] = make_loaded_file()
                loaded_file = self._thread_local.file_cache[key]
            else:
                loaded_file = make_loaded_file()

            files[file.file_id] = loaded_file
        return FileCollection(files, auto_rewind=auto_rewind)

    def load_with_kloppy(self, dataset: Dataset, **kwargs):
        files = self.load_files(dataset)
        if dataset.provider == "statsbomb":
            from kloppy import statsbomb

            try:
                return statsbomb.load(
                    event_data=(files.get_file("events")).stream,
                    lineup_data=(files.get_file("lineups")).stream,
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

    # def map(
    #     self, fn, dataset_collection: DatasetCollection, processes: Optional[int] = None
    # ):
    #     return map_in_pool(fn, dataset_collection, processes)
