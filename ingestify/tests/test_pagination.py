import pytest
from datetime import datetime, timedelta
import pytz

from ingestify.domain import Dataset, Identifier, DatasetState
from ingestify.main import get_engine


def test_iter_dataset_collection_batches(config_file):
    """Test iteration over datasets with batches using iter_dataset_collection_batches."""
    # Get engine from the fixture
    engine = get_engine(config_file, "main")
    store = engine.store
    bucket = store.bucket

    # Create 30 datasets with different creation times
    now = datetime.now(pytz.utc)

    # Save datasets with ascending created_at timestamps
    for i in range(30):
        dataset = Dataset(
            bucket=bucket,
            dataset_id=f"dataset-{i}",
            name=f"Dataset {i}",
            state="COMPLETE",
            identifier=Identifier(test_id=i),
            dataset_type="test",
            provider="test-provider",
            metadata={},
            created_at=now
            + timedelta(minutes=i),  # Each dataset created 1 minute apart
            updated_at=now + timedelta(minutes=i),
            last_modified_at=now + timedelta(minutes=i),
        )
        store.dataset_repository.save(bucket, dataset)

    # Test iteration with small batch_size (yields individual datasets)
    dataset_ids = []
    for dataset in store.iter_dataset_collection_batches(
        dataset_type="test",
        provider="test-provider",
        batch_size=5,  # Small batch size to force multiple batches
    ):
        dataset_ids.append(dataset.dataset_id)

    # Should get all 30 datasets
    assert len(dataset_ids) == 30

    # Make sure we have all datasets from 0 to 29
    expected_ids = [f"dataset-{i}" for i in range(30)]
    assert set(dataset_ids) == set(expected_ids)

    # Test iteration yielding entire DatasetCollection objects
    collections = []
    for collection in store.iter_dataset_collection_batches(
        dataset_type="test",
        provider="test-provider",
        batch_size=5,  # Small batch size to force multiple batches
        yield_dataset_collection=True,
    ):
        collections.append(collection)

    # Should have 6 collections (30 datasets / 5 per batch = 6 batches)
    assert len(collections) == 6

    # Verify total dataset count across all collections
    total_datasets = sum(len(collection) for collection in collections)
    assert total_datasets == 30

    # Test iteration with a filter that returns fewer results
    filtered_dataset_ids = []
    for dataset in store.iter_dataset_collection_batches(
        dataset_type="test",
        provider="test-provider",
        test_id=5,  # Only get dataset with test_id=5
        batch_size=10,
    ):
        filtered_dataset_ids.append(dataset.dataset_id)

    assert len(filtered_dataset_ids) == 1
    assert filtered_dataset_ids[0] == "dataset-5"


def test_dataset_state_filter(config_file):
    """Test filtering datasets by state."""
    # Get engine from the fixture
    engine = get_engine(config_file, "main")
    store = engine.store
    bucket = store.bucket

    now = datetime.now(pytz.utc)

    # Create datasets with different states
    states = [
        DatasetState.COMPLETE,
        DatasetState.PARTIAL,
        DatasetState.SCHEDULED,
        DatasetState.MISSING,
    ]
    for i in range(12):  # 3 datasets per state
        state = states[i % 4]
        dataset = Dataset(
            bucket=bucket,
            dataset_id=f"state-test-{i}",
            name=f"State Test {i}",
            state=state,
            identifier=Identifier(test_id=i),
            dataset_type="state-test",
            provider="test-provider",
            metadata={},
            created_at=now + timedelta(minutes=i),
            updated_at=now + timedelta(minutes=i),
            last_modified_at=now + timedelta(minutes=i),
        )
        store.dataset_repository.save(bucket, dataset)

    # Test filtering by a single state using enum
    complete_datasets = store.get_dataset_collection(
        dataset_type="state-test", dataset_state=DatasetState.COMPLETE
    )
    assert len(complete_datasets) == 3

    # Test filtering by a single state using string
    partial_datasets = store.get_dataset_collection(
        dataset_type="state-test", dataset_state="PARTIAL"
    )
    assert len(partial_datasets) == 3

    # Test filtering by multiple states using a list of enums
    mixed_datasets = store.get_dataset_collection(
        dataset_type="state-test",
        dataset_state=[
            DatasetState.COMPLETE,
            DatasetState.SCHEDULED,
            DatasetState.MISSING,
        ],
    )
    assert len(mixed_datasets) == 9

    # Test filtering by multiple states using a list of strings
    mixed_datasets_strings = store.get_dataset_collection(
        dataset_type="state-test", dataset_state=["COMPLETE", "SCHEDULED"]
    )
    assert len(mixed_datasets_strings) == 6

    # Test case-insensitivity
    lowercase_state_datasets = store.get_dataset_collection(
        dataset_type="state-test", dataset_state="complete"
    )
    assert len(lowercase_state_datasets) == 3

    # Test with iter_dataset_collection
    scheduled_dataset_ids = []
    for dataset in store.iter_dataset_collection_batches(
        dataset_type="state-test",
        dataset_state=DatasetState.SCHEDULED,
        batch_size=2,  # Small batch size to test pagination with filters
    ):
        scheduled_dataset_ids.append(dataset.dataset_id)
        assert dataset.state == DatasetState.SCHEDULED

    assert len(scheduled_dataset_ids) == 3
