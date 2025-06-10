import pytest
from unittest.mock import patch

from ingestify.main import get_engine


def test_store_version_tracking_new_store(config_file):
    """Test that a new store gets initialized with the current version."""
    with patch("ingestify.__version__", "1.0.0"):
        engine = get_engine(config_file)

        # Check that version was stored
        stored_version = engine.store.dataset_repository.get_store_version()
        assert stored_version == "1.0.0"


def test_store_version_tracking_existing_store_same_version(config_file):
    """Test that an existing store with same version doesn't cause issues."""
    with patch("ingestify.__version__", "1.0.0"):
        # Initialize store first time
        engine1 = get_engine(config_file)
        store1 = engine1.store

        # Open store again with same version
        engine2 = get_engine(config_file)
        store2 = engine2.store

        # Version should still be stored correctly
        stored_version = store2.dataset_repository.get_store_version()
        assert stored_version == "1.0.0"


def test_store_version_tracking_version_mismatch(config_file, caplog):
    """Test that version mismatch is logged as warning."""
    # Initialize store with version 1.0.0
    with patch("ingestify.__version__", "1.0.0"):
        engine1 = get_engine(config_file)
        store1 = engine1.store

        stored_version = store1.dataset_repository.get_store_version()
        assert stored_version == "1.0.0"

    # Open store with different version
    with patch("ingestify.__version__", "2.0.0"):
        engine2 = get_engine(config_file)
        store2 = engine2.store

        # Version should still be the original one
        stored_version = store2.dataset_repository.get_store_version()
        assert stored_version == "1.0.0"

        # Should have logged a warning about version mismatch
        assert "Store version mismatch" in caplog.text
        assert "stored=1.0.0, current=2.0.0" in caplog.text


def test_store_version_methods(config_file):
    """Test the repository version methods directly."""
    engine = get_engine(config_file)
    repo = engine.store.dataset_repository

    from ingestify import __version__

    # Initially the real version is stored
    assert repo.get_store_version() == __version__

    # Set a version
    repo.set_store_version("1.2.3")
    assert repo.get_store_version() == "1.2.3"

    # Update version
    repo.set_store_version("1.2.4")
    assert repo.get_store_version() == "1.2.4"
