import tempfile

import pytest
import os


@pytest.fixture(scope="function", autouse=True)
def datastore_dir():
    with tempfile.TemporaryDirectory() as tmpdirname:
        os.environ["TEST_DIR"] = tmpdirname
        os.environ["INGESTIFY_RUN_EAGER"] = "true"
        yield tmpdirname


@pytest.fixture(scope="function", autouse=True)
def ingestify_test_database_url(datastore_dir):
    # Only set and pop when not yet set
    if not os.environ.get("INGESTIFY_TEST_DATABASE_URL"):
        os.environ["INGESTIFY_TEST_DATABASE_URL"] = f"sqlite:///{datastore_dir}/main.db"
        yield
        os.environ.pop("INGESTIFY_TEST_DATABASE_URL")


@pytest.fixture(scope="function")
def config_file(ingestify_test_database_url):
    # Depend on ingestify_test_database_url to make sure environment variables are set in time
    return os.path.abspath(os.path.dirname(__file__) + "/config.yaml")
