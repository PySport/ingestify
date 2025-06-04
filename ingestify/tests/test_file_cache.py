import pytest
from io import BytesIO
from unittest.mock import patch
from datetime import datetime, timezone

from ingestify.main import get_engine
from ingestify.domain import Dataset, Identifier, Revision, File
from ingestify.domain.models.dataset.revision import RevisionSource, SourceType


def test_file_cache(config_file):
    """Test file caching with the with_file_cache context manager."""
    # Get engine from the fixture
    engine = get_engine(config_file, "main")
    store = engine.store

    # Create a timestamp for test data
    now = datetime.now(timezone.utc)

    # Create a test file
    test_file = File(
        file_id="test_file_id",
        data_feed_key="test_file",
        tag="test_tag",
        data_serialization_format="txt",
        storage_path="test/path",
        storage_size=100,
        storage_compression_method="none",
        created_at=now,
        modified_at=now,
        size=100,
        content_type="text/plain",
        data_spec_version="v1",
    )

    # Create a test revision with the file
    revision = Revision(
        revision_id=1,
        created_at=now,
        description="Test revision",
        modified_files=[test_file],
        source={"source_type": SourceType.MANUAL, "source_id": "test"},
    )

    # Create a test dataset with the revision
    dataset = Dataset(
        bucket="test-bucket",
        dataset_id="test-dataset",
        name="Test Dataset",
        state="COMPLETE",
        identifier=Identifier(test_id=1),
        dataset_type="test",
        provider="test-provider",
        metadata={},
        created_at=now,
        updated_at=now,
        last_modified_at=now,
        revisions=[revision],
    )

    # Create a simple pass-through reader function to replace the gzip reader
    def simple_reader(stream):
        return stream

    # Mock both the file repository and the _prepare_read_stream method
    with patch.object(
        store.file_repository, "load_content"
    ) as mock_load_content, patch.object(
        store, "_prepare_read_stream"
    ) as mock_prepare_read_stream:

        # Set up the mocks
        mock_load_content.return_value = BytesIO(b"test content")
        mock_prepare_read_stream.return_value = (simple_reader, "")

        # Test without caching - should load files twice
        store.load_files(dataset)
        store.load_files(dataset)

        # Should have called load_content twice (without caching)
        assert mock_load_content.call_count == 2

        # Reset the mock
        mock_load_content.reset_mock()

        # Test with caching - should load files only once
        with store.with_file_cache():
            store.load_files(dataset)
            store.load_files(dataset)

            # Should have called load_content only once (with caching)
            assert mock_load_content.call_count == 1

        # After exiting context, caching should be disabled
        store.load_files(dataset)

        # Should have called load_content again
        assert mock_load_content.call_count == 2
