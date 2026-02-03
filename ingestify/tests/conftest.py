import tempfile

import os
import pytest


@pytest.fixture(scope="function", autouse=True)
def datastore_dir():
    with tempfile.TemporaryDirectory() as tmpdirname:
        os.environ["TEST_DIR"] = tmpdirname
        os.environ["INGESTIFY_RUN_EAGER"] = "true"
        yield tmpdirname


@pytest.fixture(autouse=True)
def ingestify_test_database_url(datastore_dir, monkeypatch):
    key = "INGESTIFY_TEST_DATABASE_URL"

    value = os.environ.get(key)
    if value is None:
        value = f"sqlite:///{datastore_dir}/main.db"
        monkeypatch.setenv(key, value)

    return value


@pytest.fixture(autouse=True)
def clean_database(ingestify_test_database_url):
    """Clean database after each test, especially important for MySQL."""
    yield  # Let the test run first

    # Clean up after test for persistent databases (MySQL, PostgreSQL)
    db_url = os.environ.get("INGESTIFY_TEST_DATABASE_URL", "")
    if db_url.startswith("mysql://") or db_url.startswith("postgresql://"):
        from ingestify.infra.store.dataset.sqlalchemy.repository import (
            SqlAlchemySessionProvider,
        )

        provider = SqlAlchemySessionProvider(db_url)
        provider.drop_all_tables()
        provider.close()


@pytest.fixture(scope="function")
def config_file(ingestify_test_database_url):
    # Depend on ingestify_test_database_url to make sure environment variables are set in time
    return os.path.abspath(os.path.dirname(__file__) + "/config.yaml")
