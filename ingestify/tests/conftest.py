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
    if not os.environ.get("INGESTIFY_TEST_DATABASE_URL"):
        tmp_dir = os.environ["TEST_DIR"]
        os.environ["INGESTIFY_TEST_DATABASE_URL"] = f"sqlite:///${tmp_dir}/main.db"


@pytest.fixture(scope="function")
def config_file(ingestify_test_database_url):
    # Depend on ingestify_test_database_url to make sure environment variables are set in time
    return os.path.abspath(os.path.dirname(__file__) + "/config.yaml")
