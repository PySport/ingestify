from datetime import datetime
from typing import Optional

import pytz

from ingestify import Source, DatasetResource
from ingestify.application.ingestion_engine import IngestionEngine
from ingestify.domain import (
    Identifier,
    Selector,
    DataSpecVersionCollection,
    DraftFile,
    Revision,
    Dataset,
)
from ingestify.domain.models.dataset.collection_metadata import (
    DatasetCollectionMetadata,
)
from ingestify.domain.models.extract_job import ExtractJob
from ingestify.domain.models.fetch_policy import FetchPolicy
from ingestify.infra.serialization import serialize, unserialize
from ingestify.main import get_engine


def add_extract_job(engine: IngestionEngine, source: Source, **selector):
    data_spec_versions = DataSpecVersionCollection.from_dict({"default": {"v1"}})

    engine.add_extract_job(
        ExtractJob(
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
                dict(
                    competition_id=competition_id,
                    season_id=season_id,
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
                        dict(
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


def test_engine(config_file):
    engine = get_engine(config_file, "main")

    add_extract_job(
        engine, SimpleFakeSource("fake-source"), competition_id=1, season_id=2
    )
    engine.load()
    datasets = engine.store.get_dataset_collection()
    assert len(datasets) == 1

    dataset = datasets.first()
    assert dataset.identifier == Identifier(competition_id=1, season_id=2)
    assert len(dataset.revisions) == 1

    engine.load()
    datasets = engine.store.get_dataset_collection()
    assert len(datasets) == 1

    dataset = datasets.first()
    assert dataset.identifier == Identifier(competition_id=1, season_id=2)
    assert len(dataset.revisions) == 2
    assert len(dataset.revisions[0].modified_files) == 3
    assert len(dataset.revisions[1].modified_files) == 1

    add_extract_job(
        engine, SimpleFakeSource("fake-source"), competition_id=1, season_id=3
    )
    engine.load()

    datasets = engine.store.get_dataset_collection()
    assert len(datasets) == 2

    datasets = engine.store.get_dataset_collection(season_id=3)
    assert len(datasets) == 1


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

        if idx == 100:
            batch_source.should_stop = True

    batch_source = BatchSource("fake-source", callback)

    add_extract_job(engine, batch_source, competition_id=1, season_id=2)
    engine.load()

    datasets = engine.store.get_dataset_collection()
    assert len(datasets) == 100
    for dataset in datasets:
        assert len(dataset.revisions) == 1

    # Now lets run again. This should create new revisions
    batch_source.idx = 0
    batch_source.should_stop = False

    def callback(idx):
        if idx == 100:
            batch_source.should_stop = True

    batch_source.callback = callback

    engine.load()
    datasets = engine.store.get_dataset_collection()
    assert len(datasets) == 100
    for dataset in datasets:
        assert len(dataset.revisions) == 2

    # Sneaked in an extra test for serialization. This just shouldn't break
    s = serialize(datasets.first())
    unserialize(s, Dataset)
