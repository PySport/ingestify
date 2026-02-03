import tempfile

import os
import uuid

import pytest

from ingestify.infra.store.dataset.sqlalchemy.repository import (
    SqlAlchemySessionProvider,
)
from ingestify.main import get_engine


@pytest.fixture(scope="function", autouse=True)
def datastore_dir():
    with tempfile.TemporaryDirectory() as tmpdirname:
        os.environ["TEST_DIR"] = tmpdirname
        os.environ["INGESTIFY_RUN_EAGER"] = "true"
        yield tmpdirname


@pytest.fixture(scope="function")
def ingestify_test_database_url(datastore_dir, monkeypatch):
    key = "INGESTIFY_TEST_DATABASE_URL"

    value = os.environ.get(key)
    if value is None:
        value = f"sqlite:///{datastore_dir}/main.db"
        monkeypatch.setenv(key, value)

    return value


@pytest.fixture(scope="function")
def config_file(ingestify_test_database_url):
    # Depend on ingestify_test_database_url to make sure environment variables are set in time, also make sure database is
    # cleaned before ingestify opens a connection
    return os.path.abspath(os.path.dirname(__file__) + "/config.yaml")


@pytest.fixture(scope="function")
def engine(config_file):
    # Now create the engine for the test
    engine = get_engine(config_file, "main")
    # session_provider = getattr(engine.store.dataset_repository, "session_provider", None)
    # if session_provider:
    #     session_provider.close()
    #
    #     session_provider.drop_all_tables()
    #     session_provider.create_all_tables()

    yield engine
    #
    # # Close connections after test
    session_provider = getattr(
        engine.store.dataset_repository, "session_provider", None
    )
    if session_provider:
        session_provider.session.remove()
        session_provider.engine.dispose()

        session_provider.drop_all_tables()
