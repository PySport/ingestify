"""Tests for fast skip pre-check."""
from ingestify import Source, DatasetResource
from ingestify.domain import DataSpecVersionCollection, DraftFile, Selector
from ingestify.domain.models.dataset.collection_metadata import (
    DatasetCollectionMetadata,
)
from ingestify.domain.models.fetch_policy import FetchPolicy
from ingestify.domain.models.ingestion.ingestion_plan import IngestionPlan
from ingestify.utils import utcnow


def loader(file_resource, current_file, **kwargs):
    return DraftFile.from_input("data", data_feed_key="f1")


class SimpleSource(Source):
    provider = "test_provider"

    def find_datasets(
        self, dataset_type, data_spec_versions, dataset_collection_metadata, **kwargs
    ):
        for i in range(5):
            r = DatasetResource(
                dataset_resource_id={"item_id": i},
                provider=self.provider,
                dataset_type="test",
                name=f"item-{i}",
            )
            r.add_file(
                last_modified=utcnow(),
                data_feed_key="f1",
                data_spec_version="v1",
                file_loader=loader,
            )
            yield r


def _setup(engine):
    source = SimpleSource("s")
    dsv = DataSpecVersionCollection.from_dict({"default": {"v1"}})
    engine.add_ingestion_plan(
        IngestionPlan(
            source=source,
            fetch_policy=FetchPolicy(),
            dataset_type="test",
            selectors=[Selector.build({}, data_spec_versions=dsv)],
            data_spec_versions=dsv,
        )
    )


def test_timestamps_cache_matches_identifiers(engine):
    """Keys from get_existing_dataset_timestamps match Identifier.key."""
    _setup(engine)
    engine.run()

    timestamps = engine.store.get_existing_dataset_timestamps(
        provider="test_provider", dataset_type="test"
    )
    datasets = engine.store.get_dataset_collection(
        provider="test_provider", dataset_type="test"
    )

    assert len(timestamps) == len(datasets) == 5
    for dataset in datasets:
        assert dataset.identifier.key in timestamps
