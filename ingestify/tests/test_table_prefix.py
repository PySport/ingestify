#!/usr/bin/env python3
"""End-to-end test for table_prefix configuration"""
import tempfile
import yaml
from pathlib import Path
from sqlalchemy import inspect

from ingestify.main import get_datastore


def test_table_prefix_from_config():
    """Test that metadata_options.table_prefix is correctly applied from config"""
    temp_dir = Path(tempfile.mkdtemp())

    # Test 1: Config without metadata_options (default behavior)
    config_no_prefix = {
        "main": {
            "metadata_url": f"sqlite:///{temp_dir / 'no_prefix.db'}",
            "file_url": f"file://{temp_dir / 'files'}",
            "default_bucket": "main",
        }
    }
    config_path_no_prefix = temp_dir / "config_no_prefix.yaml"
    config_path_no_prefix.write_text(yaml.dump(config_no_prefix))

    store_no_prefix = get_datastore(str(config_path_no_prefix))
    inspector = inspect(store_no_prefix.dataset_repository.session_provider.engine)
    tables = inspector.get_table_names()

    assert "dataset" in tables
    assert "revision" in tables
    assert "file" in tables
    assert store_no_prefix.dataset_repository.dataset_table.name == "dataset"

    # Test 2: Config with metadata_options.table_prefix
    config_with_prefix = {
        "main": {
            "metadata_url": f"sqlite:///{temp_dir / 'with_prefix.db'}",
            "file_url": f"file://{temp_dir / 'files'}",
            "default_bucket": "main",
            "metadata_options": {"table_prefix": "prod_"},
        }
    }
    config_path_with_prefix = temp_dir / "config_with_prefix.yaml"
    config_path_with_prefix.write_text(yaml.dump(config_with_prefix))

    store_with_prefix = get_datastore(str(config_path_with_prefix))
    inspector_prefixed = inspect(
        store_with_prefix.dataset_repository.session_provider.engine
    )
    tables_prefixed = inspector_prefixed.get_table_names()

    assert "prod_dataset" in tables_prefixed
    assert "prod_revision" in tables_prefixed
    assert "prod_file" in tables_prefixed
    assert "prod_ingestion_job_summary" in tables_prefixed
    assert "prod_task_summary" in tables_prefixed
    assert "prod_store_version" in tables_prefixed
    assert store_with_prefix.dataset_repository.dataset_table.name == "prod_dataset"

    # Verify foreign keys reference prefixed tables
    revision_fks = inspector_prefixed.get_foreign_keys("prod_revision")
    assert revision_fks[0]["referred_table"] == "prod_dataset"

    file_fks = inspector_prefixed.get_foreign_keys("prod_file")
    assert file_fks[0]["referred_table"] == "prod_revision"

    task_fks = inspector_prefixed.get_foreign_keys("prod_task_summary")
    assert task_fks[0]["referred_table"] == "prod_ingestion_job_summary"

    import shutil

    shutil.rmtree(temp_dir)


if __name__ == "__main__":
    test_table_prefix_from_config()
    print("✓ All tests passed")
