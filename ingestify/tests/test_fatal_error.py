"""Tests for FatalError exception."""
from unittest.mock import patch

import pytest

from ingestify import Source, DatasetResource
from ingestify.domain import DataSpecVersionCollection, DraftFile, Selector
from ingestify.domain.models.fetch_policy import FetchPolicy
from ingestify.domain.models.ingestion.ingestion_plan import IngestionPlan
from ingestify.exceptions import FatalError
from ingestify.utils import utcnow


def good_loader(file_resource, current_file, **kwargs):
    return DraftFile.from_input("data", data_feed_key="f1")


class SourceFatalInFindDatasets(Source):
    """Source whose find_datasets fails fatally before yielding anything
    (mirrors an account-level authorization error surfacing in discovery)."""

    provider = "test_provider"

    def find_datasets(
        self, dataset_type, data_spec_versions, dataset_collection_metadata, **kwargs
    ):
        raise FatalError("account deactivated")
        yield  # pragma: no cover - makes this a generator


class SourceFatalMidStream(Source):
    """Source that yields 2 good datasets, then fails fatally."""

    provider = "test_provider"

    def find_datasets(
        self, dataset_type, data_spec_versions, dataset_collection_metadata, **kwargs
    ):
        for i in range(2):
            r = DatasetResource(
                dataset_resource_id={"item_id": i},
                provider=self.provider,
                dataset_type="test",
                name=f"item-{i}",
            )
            r.add_file(
                last_modified=utcnow(),
                data_feed_key="f1",
                data_spec_version="v1",
                file_loader=good_loader,
            )
            yield r
        raise FatalError("account deactivated")


def _setup(engine, source):
    dsv = DataSpecVersionCollection.from_dict({"default": {"v1"}})
    engine.add_ingestion_plan(
        IngestionPlan(
            source=source,
            fetch_policy=FetchPolicy(),
            dataset_type="test",
            selectors=[Selector.build({}, data_spec_versions=dsv)],
            data_spec_versions=dsv,
        )
    )


def test_fatal_error_has_exit_code():
    assert FatalError.exit_code == 1


def test_fatal_error_in_find_datasets_propagates(engine):
    """FatalError raised in find_datasets propagates out of engine.run()
    instead of being swallowed as a skipped find_datasets."""
    _setup(engine, SourceFatalInFindDatasets("s"))

    with pytest.raises(FatalError, match="deactivated"):
        engine.run()


def test_fatal_error_mid_stream_propagates(engine):
    """FatalError raised after some datasets still propagates."""
    _setup(engine, SourceFatalMidStream("s"))

    with pytest.raises(FatalError, match="deactivated"):
        engine.run()
