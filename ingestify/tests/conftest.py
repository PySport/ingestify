import tempfile

import pytest
import os


@pytest.fixture(scope="function", autouse=True)
def datastore_dir():
    with tempfile.TemporaryDirectory() as tmpdirname:
        os.environ["TEST_DIR"] = tmpdirname
        os.environ["INGESTIFY_RUN_EAGER"] = "true"

        # Allow database URL to be overridden via environment variable
        # If INGESTIFY_TEST_DATABASE_URL is not set, use SQLite by default
        if "INGESTIFY_TEST_DATABASE_URL" not in os.environ:
            os.environ[
                "INGESTIFY_TEST_DATABASE_URL"
            ] = f"sqlite:///{tmpdirname}/main.db"

        yield tmpdirname


@pytest.fixture(scope="session")
def config_file():
    return os.path.abspath(os.path.dirname(__file__) + "/config.yaml")
