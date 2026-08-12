"""Tests for invalidating revisions and refetching."""
from datetime import datetime, timezone
from pathlib import Path

from ingestify import Source, DatasetResource
from ingestify.domain import DataSpecVersionCollection, DraftFile, Selector
from ingestify.domain.models.dataset.collection_metadata import (
    DatasetCollectionMetadata,
)
from ingestify.domain.models.dataset.dataset import Dataset
from ingestify.domain.models.dataset.dataset_state import DatasetState
from ingestify.domain.models.dataset.file import File
from ingestify.domain.models.dataset.identifier import Identifier
from ingestify.domain.models.dataset.revision import Revision, RevisionState
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


def test_invalidate_multi_revision_dataset_refetches(engine):
    """A dataset with 2+ revisions must still refetch after invalidation.

    Regression: Dataset.current_revision squashes multiple revisions into a new
    Revision without a state, defaulting to PENDING_VALIDATION. This drops the
    VALIDATION_FAILED state set by invalidate_revisions, so FetchPolicy.should_refetch
    returns False and the dataset is silently skipped instead of refetched.
    """
    global call_count
    call_count = 0
    _setup(engine)

    # Build a dataset with TWO revisions: fetch -> invalidate -> refetch.
    engine.run()  # revision 0
    assert call_count == 1
    ds = list(
        engine.store.get_dataset_collection(
            provider="test_provider", dataset_type="test"
        )
    )[0]
    engine.store.invalidate_revisions([ds], reason="first")
    engine.run()  # revision 1 (refetch)
    assert call_count == 2

    ds = list(
        engine.store.get_dataset_collection(
            provider="test_provider", dataset_type="test"
        )
    )[0]
    assert len(ds.revisions) == 2, "precondition: dataset should have two revisions"

    # Invalidate again — every revision is now VALIDATION_FAILED.
    engine.store.invalidate_revisions([ds], reason="second")
    ds = list(
        engine.store.get_dataset_collection(
            provider="test_provider", dataset_type="test"
        )
    )[0]
    assert (
        ds.current_revision.state == RevisionState.VALIDATION_FAILED
    ), "squashed current_revision must preserve the VALIDATION_FAILED state"

    # And the next run must refetch it.
    engine.run()
    assert call_count == 3, "invalidated multi-revision dataset should be refetched"


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


# --- Unit tests for the squashed current_revision state -------------------


def _file(file_id: str) -> File:
    return File(
        file_id=file_id,
        created_at=FIXED_TIME,
        modified_at=FIXED_TIME,
        tag=file_id,
        size=10,
        content_type="application/json",
        data_feed_key=file_id,
        data_spec_version="v1",
        data_serialization_format="json",
        storage_size=10,
        storage_compression_method=None,
        storage_path=Path(f"/{file_id}"),
    )


def _revision(revision_id: int, files: list[File], state: RevisionState) -> Revision:
    return Revision(
        revision_id=revision_id,
        created_at=FIXED_TIME,
        description="",
        modified_files=files,
        source=None,
        state=state,
    )


def _dataset(revisions: list[Revision]) -> Dataset:
    return Dataset(
        bucket="b",
        dataset_id="d1",
        name="d",
        state=DatasetState.COMPLETE,
        dataset_type="test",
        provider="p",
        identifier=Identifier(item_id=1),
        metadata={},
        created_at=FIXED_TIME,
        updated_at=FIXED_TIME,
        revisions=revisions,
        last_modified_at=None,
    )


def test_squashed_state_failed_when_a_current_file_comes_from_a_failed_revision():
    """Two files, both fail in rev0; rev1 fixes only f1 (f2 not re-fetched).

    The squashed current view has f1 from rev1 (good) but f2 still from rev0
    (VALIDATION_FAILED). The dataset still contains invalid data, so the
    squashed state must be VALIDATION_FAILED — not the latest revision's state.
    """
    rev0 = _revision(0, [_file("f1"), _file("f2")], RevisionState.VALIDATION_FAILED)
    rev1 = _revision(1, [_file("f1")], RevisionState.PENDING_VALIDATION)
    dataset = _dataset([rev0, rev1])

    current = dataset.current_revision

    # precondition: f1 is now from rev1, f2 still from rev0
    assert current.modified_files_map["f1"].revision_id == 1
    assert current.modified_files_map["f2"].revision_id == 0

    assert current.state == RevisionState.VALIDATION_FAILED, (
        "f2 still comes from a failed revision, so the squashed state must be "
        "VALIDATION_FAILED"
    )


def test_squashed_state_ok_when_all_current_files_come_from_good_revisions():
    """Counter-case: if the failed files were all superseded by good revisions,
    the squash must NOT be VALIDATION_FAILED (otherwise it would refetch forever)."""
    rev0 = _revision(0, [_file("f1"), _file("f2")], RevisionState.VALIDATION_FAILED)
    rev1 = _revision(1, [_file("f1"), _file("f2")], RevisionState.PENDING_VALIDATION)
    dataset = _dataset([rev0, rev1])

    current = dataset.current_revision

    assert current.modified_files_map["f1"].revision_id == 1
    assert current.modified_files_map["f2"].revision_id == 1
    assert current.state != RevisionState.VALIDATION_FAILED
