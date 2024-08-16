from datetime import datetime
from typing import Optional

import pytz

from ingestify import Source
from ingestify.application.ingestion_engine import IngestionEngine
from ingestify.domain import (
    Identifier,
    Selector,
    DataSpecVersionCollection,
    DraftFile,
    Revision,
)
from ingestify.domain.models.extract_job import ExtractJob
from ingestify.domain.models.fetch_policy import FetchPolicy
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


class SimpleFakeSource(Source):
    @property
    def provider(self) -> str:
        return "fake"

    def discover_datasets(
        self,
        dataset_type: str,
        data_spec_versions: DataSpecVersionCollection,
        competition_id,
        season_id,
        **kwargs
    ):
        return [
            dict(
                competition_id=competition_id,
                season_id=season_id,
                _name="Test Dataset",
                _last_modified=datetime.now(pytz.utc),
            )
        ]

    def fetch_dataset_files(
        self,
        dataset_type: str,
        identifier: Identifier,
        data_spec_versions: DataSpecVersionCollection,
        current_revision: Optional[Revision],
    ):
        if current_revision:
            return {
                "file1": DraftFile.from_input(
                    "different_content",
                ),
                "file2": DraftFile.from_input("some_content" + identifier.key),
            }
        else:
            return {
                "file1": DraftFile.from_input(
                    "content1",
                ),
                "file2": DraftFile.from_input("some_content" + identifier.key),
            }


class BatchSource(Source):
    def __init__(self, name, callback):
        super().__init__(name)
        self.callback = callback
        self.should_stop = False
        self.idx = 0

    @property
    def provider(self) -> str:
        return "fake"

    def discover_datasets(
        self,
        dataset_type: str,
        data_spec_versions: DataSpecVersionCollection,
        competition_id,
        season_id,
        **kwargs
    ):
        while not self.should_stop:
            items = []
            for i in range(10):
                match_id = self.idx
                self.idx += 1
                item = dict(
                    competition_id=competition_id,
                    season_id=season_id,
                    match_id=match_id,
                    _name="Test Dataset",
                    _last_modified=datetime.now(pytz.utc),
                )
                items.append(item)
            yield items
            self.callback and self.callback(self.idx)

    def fetch_dataset_files(
        self,
        dataset_type: str,
        identifier: Identifier,
        data_spec_versions: DataSpecVersionCollection,
        current_revision: Optional[Revision],
    ):
        if current_revision:
            return {
                "file1": DraftFile.from_input(
                    "different_content",
                ),
                "file2": DraftFile.from_input("some_content" + identifier.key),
            }
        else:
            return {
                "file1": DraftFile.from_input(
                    "content1",
                ),
                "file2": DraftFile.from_input("some_content" + identifier.key),
            }


def test_engine():
    engine = get_engine("config.yaml", "main")

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
    assert len(dataset.revisions[0].modified_files) == 2
    assert len(dataset.revisions[1].modified_files) == 1

    add_extract_job(
        engine, SimpleFakeSource("fake-source"), competition_id=1, season_id=3
    )
    engine.load()

    datasets = engine.store.get_dataset_collection()
    assert len(datasets) == 2

    datasets = engine.store.get_dataset_collection(season_id=3)
    assert len(datasets) == 1


def test_iterator_source():
    """Test when a Source returns a Iterator to do Batch processing.

    Every batch must be executed right away.
    """
    engine = get_engine("config.yaml", "main")

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
