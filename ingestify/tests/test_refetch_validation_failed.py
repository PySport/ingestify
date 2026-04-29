"""Tests for invalidating revisions and refetching."""
from datetime import datetime, timezone

from ingestify import Source, DatasetResource
from ingestify.domain import DataSpecVersionCollection, DraftFile, Selector
from ingestify.domain.models.dataset.collection_metadata import (
    DatasetCollectionMetadata,
)
from ingestify.domain.models.dataset.revision import RevisionState
from ingestify.domain.models.fetch_policy import FetchPolicy
from ingestify.domain.models.ingestion.ingestion_plan import IngestionPlan

# Fixed timestamp so last_modified doesn't change between runs
FIXED_TIME = datetime(2026, 1, 1, tzinfo=timezone.utc)

call_count = 0


def counting_loader(file_resource, current_file, **kwargs):
    global call_count
    call_count += 1
    return DraftFile.from_input(f"data-{call_count}", data_feed_key="f1")


class SimpleSource(Source):
    provider = "test_provider"

    def __init__(self, name, n_datasets=1):
        super().__init__(name)
        self.n_datasets = n_datasets

    def find_datasets(
        self, dataset_type, data_spec_versions, dataset_collection_metadata, **kwargs
    ):
        for i in range(self.n_datasets):
            r = DatasetResource(
                dataset_resource_id={"item_id": i},
                provider=self.provider,
                dataset_type="test",
                name=f"item-{i}",
            )
            r.add_file(
                last_modified=FIXED_TIME,
                data_feed_key="f1",
                data_spec_version="v1",
                file_loader=counting_loader,
            )
            yield r


def _setup(engine):
    dsv = DataSpecVersionCollection.from_dict({"default": {"v1"}})
    engine.add_ingestion_plan(
        IngestionPlan(
            source=SimpleSource("s"),
            fetch_policy=FetchPolicy(),
            dataset_type="test",
            selectors=[Selector.build({}, data_spec_versions=dsv)],
            data_spec_versions=dsv,
        )
    )


def test_normal_second_run_skips(engine):
    """Verify a second run with same last_modified does NOT refetch."""
    global call_count
    call_count = 0
    _setup(engine)

    engine.run()
    assert call_count == 1

    engine.run()
    assert call_count == 1, "Should NOT refetch when nothing changed"


def test_invalidate_revision_triggers_refetch(engine):
    """Invalidating a revision causes ingestify to refetch on next run."""
    global call_count
    call_count = 0
    _setup(engine)

    # First run: creates the dataset
    engine.run()
    assert call_count == 1

    # Invalidate the current revision
    datasets = list(
        engine.store.get_dataset_collection(
            provider="test_provider", dataset_type="test"
        )
    )
    dataset = datasets[0]
    engine.store.invalidate_revision(dataset, reason="Data quality check failed")

    # Verify state
    datasets = list(
        engine.store.get_dataset_collection(
            provider="test_provider", dataset_type="test"
        )
    )
    assert datasets[0].current_revision.state == RevisionState.VALIDATION_FAILED

    # Second run: should refetch
    engine.run()
    assert call_count == 2, "Dataset with invalidated revision should be refetched"


def test_invalidate_revisions_batch(engine):
    """invalidate_revisions works on multiple datasets at once."""
    global call_count
    call_count = 0

    dsv = DataSpecVersionCollection.from_dict({"default": {"v1"}})
    engine.add_ingestion_plan(
        IngestionPlan(
            source=SimpleSource("s", n_datasets=5),
            fetch_policy=FetchPolicy(),
            dataset_type="test",
            selectors=[Selector.build({}, data_spec_versions=dsv)],
            data_spec_versions=dsv,
        )
    )

    # First run: creates 5 datasets
    engine.run()
    assert call_count == 5

    # Batch invalidate all 5
    datasets = list(
        engine.store.get_dataset_collection(
            provider="test_provider", dataset_type="test"
        )
    )
    assert len(datasets) == 5
    engine.store.invalidate_revisions(datasets, reason="Batch test")

    # Verify all invalidated
    datasets = list(
        engine.store.get_dataset_collection(
            provider="test_provider", dataset_type="test"
        )
    )
    for ds in datasets:
        assert ds.current_revision.state == RevisionState.VALIDATION_FAILED
        assert ds.last_modified_at is None

    # Second run: should refetch all 5
    engine.run()
    assert call_count == 10, "All 5 invalidated datasets should be refetched"
