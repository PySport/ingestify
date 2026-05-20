"""Tests for sources with submit/collect (async loading pattern)."""
from typing import Iterator

from ingestify import Source, DatasetResource
from ingestify.domain import DataSpecVersionCollection, DraftFile, Selector
from ingestify.domain.models.dataset.collection_metadata import DatasetCollectionMetadata
from ingestify.domain.models.fetch_policy import FetchPolicy
from ingestify.domain.models.ingestion.ingestion_plan import IngestionPlan
from ingestify.main import get_dev_engine
from ingestify.utils import utcnow


class FakeAsyncSource(Source):
    """Source that uses submit/collect instead of file_loader."""

    provider = "fake_async"

    def __init__(self, name, datasets, capacity=3):
        super().__init__(name)
        self._datasets = datasets
        self._capacity = capacity
        self._buffer = []
        self._submitted = {}
        self._in_flight = 0

    def find_datasets(
        self,
        dataset_type,
        data_spec_versions,
        dataset_collection_metadata,
        **kwargs,
    ):
        last_modified = utcnow()
        for keyword in self._datasets:
            yield DatasetResource(
                dataset_resource_id={"keyword": keyword},
                provider=self.provider,
                dataset_type="keyword",
                name=keyword,
            )

    def submit(self, dataset_resources: Iterator[DatasetResource]) -> bool:
        for resource in dataset_resources:
            keyword = resource.dataset_resource_id["keyword"]
            self._submitted[keyword] = resource
            self._in_flight += 1
            if self._in_flight >= self._capacity:
                return False
        return True

    def collect(self) -> Iterator[DatasetResource]:
        for keyword, resource in list(self._submitted.items()):
            resource.add_file(
                last_modified=utcnow(),
                data_feed_key="data",
                data_spec_version="v1",
                json_content={"keyword": keyword, "rank": 1},
            )
            del self._submitted[keyword]
            self._in_flight -= 1
            yield resource

    def has_pending(self) -> bool:
        return self._in_flight > 0


def _make_engine(source, tmp_path):
    engine = get_dev_engine(
        source=source,
        dataset_type="keyword",
        data_spec_versions={"default": "v1"},
        dev_dir=str(tmp_path),
        configure_logging=False,
    )
    return engine


def test_async_source_basic(tmp_path):
    """Source with submit/collect ingests all datasets with files."""
    source = FakeAsyncSource("test", ["alpha", "beta", "gamma"])
    engine = _make_engine(source, tmp_path)
    engine.run()

    datasets = list(engine.store.get_dataset_collection(dataset_type="keyword"))
    assert len(datasets) == 3
    keywords = {d.identifier["keyword"] for d in datasets}
    assert keywords == {"alpha", "beta", "gamma"}

    # Verify files were actually stored
    for ds in datasets:
        files = engine.store.load_files(ds)
        loaded = files.get_file("data")
        assert loaded is not None, f"Dataset {ds.identifier} has no 'data' file"


def test_async_source_with_capacity(tmp_path):
    """submit returns False when capacity is reached, collect frees capacity."""
    source = FakeAsyncSource("test", ["a", "b", "c", "d", "e"], capacity=2)
    engine = _make_engine(source, tmp_path)
    engine.run()

    datasets = list(engine.store.get_dataset_collection(dataset_type="keyword"))
    assert len(datasets) == 5
    for ds in datasets:
        files = engine.store.load_files(ds)
        assert files.get_file("data") is not None


def test_async_source_skips_existing(tmp_path):
    """Second run skips already-ingested datasets."""
    source = FakeAsyncSource("test", ["alpha", "beta"])
    engine = _make_engine(source, tmp_path)

    # First run
    engine.run()
    datasets = list(engine.store.get_dataset_collection(dataset_type="keyword"))
    assert len(datasets) == 2

    # Second run — same data, should skip
    engine.run()
    datasets = list(engine.store.get_dataset_collection(dataset_type="keyword"))
    assert len(datasets) == 2
    # Each dataset should have exactly 1 revision (not re-fetched)
    for ds in datasets:
        assert len(ds.revisions) == 1


def test_async_source_with_existing_files(tmp_path):
    """find_datasets can attach files; collect adds more."""

    class SourceWithExistingFiles(Source):
        provider = "fake_async"

        def find_datasets(self, dataset_type, data_spec_versions,
                          dataset_collection_metadata, **kwargs):
            yield (
                DatasetResource(
                    dataset_resource_id={"keyword": "test"},
                    provider=self.provider,
                    dataset_type="keyword",
                    name="test",
                ).add_file(
                    last_modified=utcnow(),
                    data_feed_key="metadata",
                    data_spec_version="v1",
                    json_content={"source": "find_datasets"},
                )
            )

        def submit(self, dataset_resources):
            self._resources = []
            for r in dataset_resources:
                self._resources.append(r)
            return True

        def collect(self):
            for resource in self._resources:
                resource.add_file(
                    last_modified=utcnow(),
                    data_feed_key="serp",
                    data_spec_version="v1",
                    json_content={"source": "collect", "rank": 1},
                )
                yield resource
            self._resources = []

        def has_pending(self):
            return bool(self._resources)

    source = SourceWithExistingFiles("test")
    engine = _make_engine(source, tmp_path)
    engine.run()

    datasets = list(engine.store.get_dataset_collection(dataset_type="keyword"))
    assert len(datasets) == 1

    files = engine.store.load_files(datasets[0])
    assert files.get_file("metadata") is not None
    assert files.get_file("serp") is not None
