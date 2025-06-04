import pytest
from datetime import datetime, timedelta
import pytz

from ingestify.domain import Dataset, Identifier
from ingestify.main import get_engine


def test_iter_dataset_collection(config_file):
    """Test iteration over datasets with pagination using iter_dataset_collection."""
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

    # Test iteration with small page_size (yields individual datasets)
    dataset_ids = []
    for dataset in store.iter_dataset_collection(
        dataset_type="test",
        provider="test-provider",
        page_size=5,  # Small page size to force multiple pages
    ):
        dataset_ids.append(dataset.dataset_id)

    # Should get all 30 datasets
    assert len(dataset_ids) == 30

    # Make sure we have all datasets from 0 to 29
    expected_ids = [f"dataset-{i}" for i in range(30)]
    assert set(dataset_ids) == set(expected_ids)

    # Test iteration yielding entire DatasetCollection objects
    collections = []
    for collection in store.iter_dataset_collection(
        dataset_type="test",
        provider="test-provider",
        page_size=5,  # Small page size to force multiple pages
        yield_dataset_collection=True,
    ):
        collections.append(collection)

    # Should have 6 collections (30 datasets / 5 per page = 6 pages)
    assert len(collections) == 6

    # Verify total dataset count across all collections
    total_datasets = sum(len(collection) for collection in collections)
    assert total_datasets == 30

    # Test iteration with a filter that returns fewer results
    filtered_dataset_ids = []
    for dataset in store.iter_dataset_collection(
        dataset_type="test",
        provider="test-provider",
        test_id=5,  # Only get dataset with test_id=5
        page_size=10,
    ):
        filtered_dataset_ids.append(dataset.dataset_id)

    assert len(filtered_dataset_ids) == 1
    assert filtered_dataset_ids[0] == "dataset-5"
