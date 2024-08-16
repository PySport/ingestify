import tempfile

import pytest
import os


@pytest.fixture(scope="function", autouse=True)
def datastore_dir():
    with tempfile.TemporaryDirectory() as tmpdirname:
        os.environ["TEST_DIR"] = tmpdirname
        os.environ["INGESTIFY_RUN_EAGER"] = "true"
        yield tmpdirname


@pytest.fixture(scope="session")
def config_file():
    return os.path.abspath(os.path.dirname(__file__) + "/config.yaml")
