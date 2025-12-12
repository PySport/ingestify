from datetime import datetime

import pytz

from ingestify import Source, DatasetResource
from ingestify.application.ingestion_engine import IngestionEngine
from ingestify.domain import (
    Identifier,
    Selector,
    DataSpecVersionCollection,
    DraftFile,
    Dataset,
    DatasetState,
    DatasetCreated,
)
from ingestify.domain.models.dataset.collection_metadata import (
    DatasetCollectionMetadata,
)
from ingestify.domain.models.dataset.events import RevisionAdded
from ingestify.domain.models.ingestion.ingestion_job_summary import (
    IngestionJobSummary,
    IngestionJobState,
)
from ingestify.domain.models.ingestion.ingestion_plan import IngestionPlan
from ingestify.domain.models.fetch_policy import FetchPolicy
from ingestify.domain.models.task.task_summary import TaskState
from ingestify.infra.serialization import serialize, deserialize
from ingestify.main import get_engine, get_dev_engine


def add_ingestion_plan(engine: IngestionEngine, source: Source, **selector):
    data_spec_versions = DataSpecVersionCollection.from_dict({"default": {"v1"}})

    engine.add_ingestion_plan(
        IngestionPlan(
            source=source,
            fetch_policy=FetchPolicy(),
            selectors=[Selector.build(selector, data_spec_versions=data_spec_versions)],
            dataset_type="match",
            data_spec_versions=data_spec_versions,
        )
    )


def file_loader(file_resource, current_file, some_extract_config=None):
    if some_extract_config is not None and some_extract_config != "test123":
        # Test loader_kwargs are passed correctly
        raise Exception(f"Incorrect value for this test value: {some_extract_config}")

    if file_resource.file_id == "file1__v1":
        if not current_file:
            return DraftFile.from_input(
                "content1",
                data_feed_key="file1",
            )
        else:
            return DraftFile.from_input(
                "different_content",
                data_feed_key="file1",
            )

    elif file_resource.file_id == "file2__v1":
        return DraftFile.from_input(
            "some_content" + str(file_resource.dataset_resource.dataset_resource_id),
            data_feed_key="file2",
        )


class SimpleFakeSource(Source):
    provider = "fake"

    def find_datasets(
        self,
        dataset_type: str,
        data_spec_versions: DataSpecVersionCollection,
        dataset_collection_metadata: DatasetCollectionMetadata,
        competition_id,
        season_id,
        **kwargs,
    ):
        last_modified = datetime.now(pytz.utc)

        yield (
            DatasetResource(
                dataset_resource_id=dict(
                    competition_id=competition_id, season_id=season_id, match_id=1
                ),
                provider="fake",
                dataset_type="match",
                name="Test Dataset",
            )
            .add_file(
                last_modified=last_modified,
                data_feed_key="file1",
                data_spec_version="v1",
                file_loader=file_loader,
                loader_kwargs={"some_extract_config": "test123"},
            )
            .add_file(
                last_modified=last_modified,
                data_feed_key="file2",
                data_spec_version="v1",
                file_loader=file_loader,
            )
            .add_file(
                last_modified=last_modified,
                data_feed_key="file3",
                data_spec_version="v1",
                json_content={"test": "some-content"},
            )
        )
        # dataset_resource.add_file(
        #     last_modified=last_modified,
        #     data_feed_key="file4",
        #     data_spec_version="v1",
        #     url="https://raw.githubusercontent.com/statsbomb/open-data/refs/heads/master/data/three-sixty/3788741.json",
        #     data_serialization_format="json"
        # )


class EmptyDatasetResourceIdSource(Source):
    provider = "fake"

    def find_datasets(
        self,
        dataset_type: str,
        data_spec_versions: DataSpecVersionCollection,
        dataset_collection_metadata: DatasetCollectionMetadata,
        **kwargs,
    ):
        last_modified = datetime.now(pytz.utc)

        yield (
            DatasetResource(
                dataset_resource_id={},
                provider="fake",
                dataset_type="match",
                name="Test Dataset",
            ).add_file(
                last_modified=last_modified,
                data_feed_key="file3",
                data_spec_version="v1",
                json_content={"test": "some-content"},
            )
        )


class BatchSource(Source):
    provider = "batch"

    def __init__(self, name, callback):
        super().__init__(name)
        self.callback = callback
        self.should_stop = False
        self.idx = 0

    def find_datasets(
        self,
        dataset_type: str,
        data_spec_versions: DataSpecVersionCollection,
        dataset_collection_metadata: DatasetCollectionMetadata,
        competition_id,
        season_id,
        **kwargs,
    ):
        while not self.should_stop:
            items = []
            for i in range(10):
                match_id = self.idx
                self.idx += 1

                last_modified = datetime.now(pytz.utc)
                dataset_resource = (
                    DatasetResource(
                        dataset_resource_id=dict(
                            competition_id=competition_id,
                            season_id=season_id,
                            match_id=match_id,
                        ),
                        name="Test dataset",
                        provider="fake",
                        dataset_type="match",
                    )
                    .add_file(
                        last_modified=last_modified,
                        data_feed_key="file1",
                        data_spec_version="v1",
                        file_loader=file_loader,
                    )
                    .add_file(
                        last_modified=last_modified,
                        data_feed_key="file2",
                        data_spec_version="v1",
                        file_loader=file_loader,
                    )
                )

                items.append(dataset_resource)
            yield items

            self.callback and self.callback(self.idx)


class FailingLoadSource(Source):
    provider = "fake"

    def find_datasets(
        self,
        dataset_type: str,
        data_spec_versions: DataSpecVersionCollection,
        dataset_collection_metadata: DatasetCollectionMetadata,
        competition_id,
        season_id,
        **kwargs,
    ):
        last_modified = datetime.now(pytz.utc)

        def failing_loader(*args, **kwargs):
            raise Exception("This is a failing task")

        yield (
            DatasetResource(
                dataset_resource_id=dict(
                    competition_id=competition_id, season_id=season_id, match_id=1
                ),
                provider="fake",
                dataset_type="match",
                name="Test Dataset",
            ).add_file(
                last_modified=last_modified,
                data_feed_key="file1",
                data_spec_version="v1",
                file_loader=failing_loader,
                loader_kwargs={"some_extract_config": "test123"},
            )
        )


class FailingJobSource(Source):
    provider = "fake"

    def find_datasets(
        self,
        dataset_type: str,
        data_spec_versions: DataSpecVersionCollection,
        dataset_collection_metadata: DatasetCollectionMetadata,
        competition_id,
        season_id,
        **kwargs,
    ):
        raise Exception("some failure")


class NoFilesSource(Source):
    provider = "fake"

    def find_datasets(
        self,
        dataset_type: str,
        data_spec_versions: DataSpecVersionCollection,
        dataset_collection_metadata: DatasetCollectionMetadata,
        competition_id,
        season_id,
        **kwargs,
    ):
        yield DatasetResource(
            dataset_resource_id=dict(
                competition_id=competition_id, season_id=season_id, match_id=1
            ),
            provider="fake",
            dataset_type="match",
            name="Dataset Without Files",
        )


def test_engine(config_file):
    engine = get_engine(config_file, "main")

    add_ingestion_plan(
        engine, SimpleFakeSource("fake-source"), competition_id=1, season_id=2
    )
    engine.load()
    datasets = engine.store.get_dataset_collection()
    assert len(datasets) == 1

    dataset = datasets.first()
    assert dataset.identifier == Identifier(competition_id=1, season_id=2, match_id=1)
    assert len(dataset.revisions) == 1

    engine.load()
    datasets = engine.store.get_dataset_collection()
    assert len(datasets) == 1

    dataset = datasets.first()
    assert dataset.identifier == Identifier(competition_id=1, season_id=2, match_id=1)
    assert len(dataset.revisions) == 2
    assert len(dataset.revisions[0].modified_files) == 3
    assert len(dataset.revisions[1].modified_files) == 1

    add_ingestion_plan(
        engine, SimpleFakeSource("fake-source"), competition_id=1, season_id=3
    )
    engine.load()

    datasets = engine.store.get_dataset_collection()
    assert len(datasets) == 2

    datasets = engine.store.get_dataset_collection(season_id=3)
    assert len(datasets) == 1

    # Make sure everything still works with a fresh connection
    engine.store.dataset_repository.session_provider.reset()

    # TODO: reenable
    # items = list(engine.store.dataset_repository.session.query(IngestionJobSummary))
    # print(items)

    # Make sure we can load the files
    files = engine.store.load_files(datasets.first(), lazy=True)
    assert files.get_file("file1").stream.read() == b"content1"

    files = engine.store.load_files(datasets.first(), lazy=False)
    assert files.get_file("file1").stream.read() == b"content1"

    assert dataset.last_modified_at is not None


def test_iterator_source(config_file):
    """Test when a Source returns a Iterator to do Batch processing.

    Every batch must be executed right away.
    """
    engine = get_engine(config_file, "main")

    batch_source = None

    def callback(idx):
        nonlocal batch_source
        datasets = engine.store.get_dataset_collection()
        assert len(datasets) == idx

        if idx == 1000:
            batch_source.should_stop = True

    batch_source = BatchSource("fake-source", callback)

    add_ingestion_plan(engine, batch_source, competition_id=1, season_id=2)
    engine.load()

    datasets = engine.store.get_dataset_collection()
    assert len(datasets) == 1000
    for dataset in datasets:
        assert len(dataset.revisions) == 1

    # Now lets run again. This should create new revisions
    batch_source.idx = 0
    batch_source.should_stop = False

    def callback(idx):
        if idx == 1000:
            batch_source.should_stop = True

    batch_source.callback = callback

    engine.load()
    datasets = engine.store.get_dataset_collection()
    assert len(datasets) == 1000
    for dataset in datasets:
        assert len(dataset.revisions) == 2

    # Sneaked in an extra test for serialization. This just shouldn't break
    s = serialize(DatasetCreated(dataset=datasets.first()))
    deserialize(s)


def test_ingestion_plan_failing_task(config_file):
    engine = get_engine(config_file, "main")

    source = FailingLoadSource("fake-source")

    add_ingestion_plan(engine, source, competition_id=1, season_id=2)
    engine.load()

    items = engine.store.dataset_repository.load_ingestion_job_summaries()
    assert len(items) == 1
    assert items[0].state == IngestionJobState.FINISHED
    assert items[0].task_summaries[0].state == TaskState.FAILED


def test_ingestion_plan_failing_job(config_file):
    engine = get_engine(config_file, "main")

    source = FailingJobSource("fake-source")

    add_ingestion_plan(engine, source, competition_id=1, season_id=2)
    engine.load()

    items = engine.store.dataset_repository.load_ingestion_job_summaries()
    assert len(items) == 1
    assert items[0].state == IngestionJobState.FAILED

    # The timing of second task should contain the exception
    assert items[0].timings[1].metadata["result"]["message"] == "some failure"
    assert items[0].timings[1].metadata["result"]["type"] == "Exception"


def test_change_partition_key_transformer():
    """When the partition key transformer is changed after a file is written, it
    must still be possible to read an existing file.

    This probably means we need to use the storage_path for reading.
    """


def test_serde(config_file):
    engine = get_engine(config_file, "main")

    add_ingestion_plan(
        engine, SimpleFakeSource("fake-source"), competition_id=1, season_id=2
    )
    engine.load()
    datasets = engine.store.get_dataset_collection()
    dataset = datasets.first()

    for event_cls in [DatasetCreated, RevisionAdded]:
        event = event_cls(dataset=dataset)

        event_dict = serialize(event)

        assert event != event_dict

        deserialized_event = deserialize(event_dict)

        assert event.model_dump_json() == deserialized_event.model_dump_json()


def test_empty_dataset_resource_id(config_file):
    """When a empty DatasetResourceId is passed nothing should break"""
    engine = get_engine(config_file, "main")

    add_ingestion_plan(engine, EmptyDatasetResourceIdSource("fake-source"))
    engine.load()


def test_dev_engine():
    """Test dev engine helper for easy development without config file"""
    source = SimpleFakeSource("test-source")

    engine = get_dev_engine(
        source=source,
        dataset_type="match",
        data_spec_versions={"default": "v1"},
        ephemeral=True,
    )

    engine.run(competition_id=1, season_id=2)

    datasets = engine.store.get_dataset_collection()
    assert len(datasets) == 1
    assert datasets.first().name == "Test Dataset"


def post_load_hook(
    dataset_resource: DatasetResource, files: dict[str, DraftFile], existing_dataset
):
    # Change state to COMPLETE if file content is not '{}'
    for file in files.values():
        if file.size > 2:
            dataset_resource.state = DatasetState.COMPLETE
            break


def file_loader_with_hook(file_resource, current_file):
    # First run: empty JSON, second run: actual data
    content = "{}" if not current_file else '{"data": "value"}'
    return DraftFile.from_input(content, data_feed_key="file1")


class SourceWithHook(Source):
    provider = "test"

    def find_datasets(
        self,
        dataset_type: str,
        data_spec_versions: DataSpecVersionCollection,
        dataset_collection_metadata,
        competition_id,
        season_id,
        **kwargs,
    ):
        last_modified = datetime.now(pytz.utc)

        yield (
            DatasetResource(
                dataset_resource_id=dict(
                    competition_id=competition_id, season_id=season_id, match_id=1
                ),
                provider="test",
                dataset_type="match",
                name="Test Dataset",
                state=DatasetState.SCHEDULED,
                post_load_files=post_load_hook,
            ).add_file(
                last_modified=last_modified,
                data_feed_key="file1",
                data_spec_version="v1",
                file_loader=file_loader_with_hook,
            )
        )


def test_post_load_files_hook(config_file):
    """Test that post_load_files hook changes state from SCHEDULED to COMPLETE when content is not empty."""
    engine = get_engine(config_file, "main")
    add_ingestion_plan(engine, SourceWithHook("test"), competition_id=1, season_id=2)

    # First run: file contains '{}', state should remain SCHEDULED
    engine.load()
    dataset1 = engine.store.get_dataset_collection().first()
    assert dataset1.state == DatasetState.SCHEDULED

    # Second run: file contains actual data, state should change to COMPLETE
    engine.load()
    dataset2 = engine.store.get_dataset_collection().first()
    assert dataset2.state == DatasetState.COMPLETE


def test_force_save_creates_revision(config_file):
    """Test that datasets get a revision even when no files are persisted."""
    engine = get_engine(config_file, "main")

    # Create one dataset with files and one without
    add_ingestion_plan(
        engine, SimpleFakeSource("fake-source"), competition_id=1, season_id=2
    )
    add_ingestion_plan(
        engine, NoFilesSource("fake-source"), competition_id=1, season_id=3
    )

    engine.load()

    # This should not fail even though one dataset has no last_modified_at
    datasets = engine.store.get_dataset_collection()
    assert len(datasets) == 2

    # Verify the dataset without files still has a revision
    dataset_without_files = engine.store.get_dataset_collection(season_id=3).first()
    assert len(dataset_without_files.revisions) == 1
    assert len(dataset_without_files.current_revision.modified_files) == 0
