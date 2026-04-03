"""Tests for create_identifier_indexes / sync-indexes functionality."""
import os

import pytest
import sqlalchemy

from ingestify.application.dataset_store import DatasetStore
from ingestify.infra.store.dataset.sqlalchemy.repository import (
    SqlAlchemySessionProvider,
    SqlAlchemyDatasetRepository,
)


INDEX_CONFIGS = [
    {
        "name": "test_keyword_metrics",
        "provider": "test",
        "dataset_type": "keyword_metrics",
        "keys": [{"name": "keyword", "key_type": "str"}],
    },
    {
        "name": "test_keyword_set",
        "provider": "test",
        "dataset_type": "keyword_set",
        "keys": [
            {"name": "dataset_id", "key_type": "int"},
            {"name": "table_name", "key_type": "str"},
        ],
    },
]


@pytest.fixture
def repository(ingestify_test_database_url, db_cleanup):
    provider = SqlAlchemySessionProvider(ingestify_test_database_url)
    repo = SqlAlchemyDatasetRepository(provider)
    yield repo
    # Drop test indexes so they don't leak between runs
    if provider.engine.dialect.name == "postgresql":
        with provider.engine.connect() as conn:
            for config in INDEX_CONFIGS:
                conn.execute(
                    sqlalchemy.text(
                        f"DROP INDEX IF EXISTS idx_dataset_identifier_{config['name']}"
                    )
                )
            conn.commit()
    provider.drop_all_tables()


def test_create_identifier_indexes_sqlite_noop(repository):
    """No-op on non-Postgres databases — must not raise."""
    repository.create_identifier_indexes(INDEX_CONFIGS)


def test_create_identifier_indexes_creates_indexes(repository):
    """Creates composite expression indexes on Postgres; skipped on other DBs."""
    if repository.session_provider.engine.dialect.name != "postgresql":
        pytest.skip("Expression index test requires PostgreSQL")

    repository.create_identifier_indexes(INDEX_CONFIGS)

    with repository.session_provider.engine.connect() as conn:
        result = conn.execute(
            sqlalchemy.text(
                "SELECT indexname FROM pg_indexes "
                "WHERE tablename = 'dataset' AND indexname LIKE 'idx_dataset_identifier_%'"
            )
        )
        index_names = {row[0] for row in result}

    assert "idx_dataset_identifier_test_keyword_metrics" in index_names
    assert "idx_dataset_identifier_test_keyword_set" in index_names


def test_create_identifier_indexes_idempotent(repository):
    """Running twice does not raise (IF NOT EXISTS)."""
    repository.create_identifier_indexes(INDEX_CONFIGS)
    repository.create_identifier_indexes(INDEX_CONFIGS)


def test_dataset_store_create_indexes(engine):
    """DatasetStore.create_indexes() delegates to the repository."""
    engine.store._identifier_index_configs = INDEX_CONFIGS
    engine.store.create_indexes()


def test_dataset_store_create_indexes_empty(engine):
    """create_indexes() with empty configs is a no-op."""
    engine.store._identifier_index_configs = []
    engine.store.create_indexes()
