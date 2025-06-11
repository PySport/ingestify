import datetime

import pytest
from unittest.mock import MagicMock

from ingestify.main import get_engine
from ingestify.domain.models.ingestion.ingestion_plan import IngestionPlan
from ingestify.domain.models.fetch_policy import FetchPolicy
from ingestify.domain import Selector, DataSpecVersionCollection
from ingestify import Source, DatasetResource


class MockSource(Source):
    """Simple mock source for basic testing."""

    @property
    def provider(self) -> str:
        return "test_provider"

    def find_datasets(
        self,
        dataset_type: str,
        data_spec_versions: DataSpecVersionCollection,
        dataset_collection_metadata,
        competition_id: int,
        **kwargs,
    ):
        # Return mock datasets for competition_id=11
        if competition_id == 11:
            yield DatasetResource(
                dataset_resource_id={
                    "competition_id": 11,
                    "season_id": 90,
                    "match_id": 1,
                },
                name="Mock match",
                dataset_type="match",
                provider=self.provider,
                url="http://test.com/match1",
            ).add_file(
                data_feed_key="test",
                last_modified=datetime.datetime.now(),
                json_content={"blaat": "piet"},
            )

        return []


class MockSourceWithDiscoverSelectors(Source):
    """Mock source that supports discover_selectors for testing."""

    @property
    def provider(self) -> str:
        return "test_provider_discover"

    def find_datasets(
        self,
        dataset_type: str,
        data_spec_versions: DataSpecVersionCollection,
        dataset_collection_metadata,
        competition_id: int,
        **kwargs,
    ):
        # Return mock datasets for specific competition_ids
        if competition_id == 11:
            yield DatasetResource(
                dataset_resource_id={
                    "competition_id": 11,
                    "season_id": 90,
                    "match_id": 1,
                },
                name="Mock match comp 11",
                dataset_type="match",
                provider=self.provider,
                url="http://test.com/match1",
            ).add_file(
                data_feed_key="test",
                last_modified=datetime.datetime.now(),
                json_content={"competition_id": 11},
            )
        elif competition_id == 22:
            yield DatasetResource(
                dataset_resource_id={
                    "competition_id": 22,
                    "season_id": 91,
                    "match_id": 2,
                },
                name="Mock match comp 22",
                dataset_type="match",
                provider=self.provider,
                url="http://test.com/match2",
            ).add_file(
                data_feed_key="test",
                last_modified=datetime.datetime.now(),
                json_content={"competition_id": 22},
            )

        return []

    def discover_selectors(self, dataset_type: str):
        """Return multiple selectors that will be filtered by user criteria."""
        return [
            {"competition_id": 11, "season_id": 90},
            {"competition_id": 22, "season_id": 91},
            {"competition_id": 33, "season_id": 92},
        ]


def test_iter_datasets_basic_auto_ingest(config_file):
    """Test basic auto-ingest functionality."""
    engine = get_engine(config_file)

    # Add a simple ingestion plan
    mock_source = MockSource(name="test_source")
    data_spec_versions = DataSpecVersionCollection.from_dict({"default": {"v1"}})

    plan = IngestionPlan(
        source=mock_source,
        fetch_policy=FetchPolicy(),
        selectors=[
            Selector.build(
                {"competition_id": 11}, data_spec_versions=data_spec_versions
            )
        ],
        dataset_type="match",
        data_spec_versions=data_spec_versions,
    )
    engine.add_ingestion_plan(plan)

    # Test auto-ingest
    datasets = list(
        engine.iter_datasets(
            provider="test_provider",
            dataset_type="match",
            competition_id=11,
            auto_ingest=True,
        )
    )

    assert len(datasets) > 0
    assert datasets[0].identifier["competition_id"] == 11


def test_iter_datasets_auto_ingest_disabled(config_file):
    """Test that auto_ingest=False returns only existing datasets."""
    engine = get_engine(config_file)

    # Should only return existing datasets (none in empty store)
    datasets = list(engine.iter_datasets(competition_id=11, auto_ingest=False))

    assert len(datasets) == 0


def test_iter_datasets_outside_config_scope(config_file):
    """Test that requests outside IngestionPlan scope return nothing."""
    engine = get_engine(config_file)

    # Add plan only for competition_id=11
    mock_source = MockSource(name="test_source")
    data_spec_versions = DataSpecVersionCollection.from_dict({"default": {"v1"}})

    plan = IngestionPlan(
        source=mock_source,
        fetch_policy=FetchPolicy(),
        selectors=[
            Selector.build(
                {"competition_id": 11}, data_spec_versions=data_spec_versions
            )
        ],
        dataset_type="match",
        data_spec_versions=data_spec_versions,
    )
    engine.add_ingestion_plan(plan)

    # Request data outside plan scope
    datasets = list(
        engine.iter_datasets(competition_id=999, auto_ingest=True)  # Not in plan
    )

    assert len(datasets) == 0


def test_iter_datasets_discover_selectors_with_filters(config_file):
    """Test that selector_filters are applied after discover_selectors runs."""
    engine = get_engine(config_file)

    # Create an IngestionPlan with empty selector - this will trigger discover_selectors
    mock_source = MockSourceWithDiscoverSelectors(name="test_source_discover")
    data_spec_versions = DataSpecVersionCollection.from_dict({"default": {"v1"}})

    plan = IngestionPlan(
        source=mock_source,
        fetch_policy=FetchPolicy(),
        selectors=[
            Selector.build({}, data_spec_versions=data_spec_versions)
        ],  # Empty selector - will use discover_selectors
        dataset_type="match",
        data_spec_versions=data_spec_versions,
    )
    engine.add_ingestion_plan(plan)

    # Test that selector_filters are applied AFTER discover_selectors
    # The source discovers 3 selectors (comp 11, 22, 33) but we filter for only comp 11
    datasets = list(
        engine.iter_datasets(
            provider="test_provider_discover",
            dataset_type="match",
            competition_id=11,  # This filter should be applied after discover_selectors
            auto_ingest=True,
        )
    )

    # Should only get datasets for competition_id=11, not all discovered selectors
    assert len(datasets) == 1
    assert datasets[0].identifier["competition_id"] == 11
    assert datasets[0].name == "Mock match comp 11"


def test_iter_datasets_discover_selectors_multiple_matches(config_file):
    """Test that multiple discovered selectors can match the filters."""
    engine = get_engine(config_file)

    # Create an IngestionPlan with empty selector - this will trigger discover_selectors
    mock_source = MockSourceWithDiscoverSelectors(name="test_source_discover")
    data_spec_versions = DataSpecVersionCollection.from_dict({"default": {"v1"}})

    plan = IngestionPlan(
        source=mock_source,
        fetch_policy=FetchPolicy(),
        selectors=[
            Selector.build({}, data_spec_versions=data_spec_versions)
        ],  # Empty selector - will use discover_selectors
        dataset_type="match",
        data_spec_versions=data_spec_versions,
    )
    engine.add_ingestion_plan(plan)

    # Test with no specific filters - should get all discovered selectors that have data
    datasets = list(
        engine.iter_datasets(
            provider="test_provider_discover", dataset_type="match", auto_ingest=True
        )
    )

    # Should get datasets for competition_ids 11 and 22 (33 has no mock data)
    assert len(datasets) == 2
    competition_ids = {d.identifier["competition_id"] for d in datasets}
    assert competition_ids == {11, 22}


def test_selector_filters_make_discovered_selectors_more_strict(config_file):
    """Test that when selector_filters are more strict than discovered selectors, we make the selectors more strict."""
    from unittest.mock import Mock

    engine = get_engine(config_file)

    # Create a source that returns multiple matches per season
    class MockSourceMultipleMatches(Source):
        @property
        def provider(self) -> str:
            return "test_multi_provider"

        def find_datasets(
            self,
            dataset_type,
            data_spec_versions,
            dataset_collection_metadata,
            **kwargs,
        ):
            competition_id = kwargs.get("competition_id")
            season_id = kwargs.get("season_id")
            match_id = kwargs.get("match_id")

            if competition_id == 11 and season_id == 90:
                # Return all matches in the season
                all_matches = [123, 124, 125]
                for mid in all_matches:
                    # Filter by match_id if specified, otherwise yield all
                    if match_id is not None and mid != match_id:
                        continue

                    yield DatasetResource(
                        dataset_resource_id={
                            "competition_id": 11,
                            "season_id": 90,
                            "match_id": mid,
                        },
                        name=f"Match {mid}",
                        dataset_type="match",
                        provider=self.provider,
                        url=f"http://test.com/match{mid}",
                    ).add_file(
                        data_feed_key="test",
                        last_modified=datetime.datetime.now(),
                        json_content={"match_id": mid},
                    )
            return []

        def discover_selectors(self, dataset_type):
            # Returns broad selector - just competition + season, no specific match
            return [
                {
                    "competition_id": 11,
                    "season_id": 90,
                },  # This would fetch ALL matches in season
            ]

    mock_source = MockSourceMultipleMatches(name="multi_source")
    original_find_datasets = mock_source.find_datasets
    mock_source.find_datasets = Mock(side_effect=original_find_datasets)

    data_spec_versions = DataSpecVersionCollection.from_dict({"default": {"v1"}})

    plan = IngestionPlan(
        source=mock_source,
        fetch_policy=FetchPolicy(),
        selectors=[Selector.build({}, data_spec_versions=data_spec_versions)],
        dataset_type="match",
        data_spec_versions=data_spec_versions,
    )
    engine.add_ingestion_plan(plan)

    # User requests specific match - more strict than discovered selector
    datasets = list(
        engine.iter_datasets(
            provider="test_multi_provider",
            dataset_type="match",
            competition_id=11,
            season_id=90,
            match_id=123,  # This should make the selector more strict
            auto_ingest=True,
        )
    )

    # Should get only the specific match, not all matches in the season
    assert len(datasets) == 1
    assert datasets[0].identifier["match_id"] == 123

    # Verify find_datasets was called with the strict parameters including match_id
    assert mock_source.find_datasets.call_count == 1
    call_args = mock_source.find_datasets.call_args_list[0]
    args, kwargs = call_args

    assert kwargs["competition_id"] == 11
    assert kwargs["season_id"] == 90
    assert kwargs["match_id"] == 123  # Should be added to avoid fetching all matches

    # Without this optimization, we'd call with match_id=None and fetch 3 matches instead of 1


def test_iter_datasets_with_open_data_auto_discovery(config_file):
    """Test that use_open_data=True auto-discovers open data sources without configuration."""
    from unittest.mock import Mock
    from ingestify.application import loader

    engine = get_engine(config_file)

    # Create mock source class that inherits from Source
    class MockOpenDataSource(Source):
        def __init__(self, name):
            super().__init__(name)

        @property
        def provider(self):
            return "statsbomb"

        def discover_selectors(self, dataset_type):
            return [{"competition_id": 11, "season_id": 90}]

        def find_datasets(
            self,
            dataset_type,
            data_spec_versions,
            dataset_collection_metadata,
            **kwargs,
        ):
            if kwargs.get("competition_id") == 11 and kwargs.get("season_id") == 90:
                yield DatasetResource(
                    dataset_resource_id={
                        "competition_id": 11,
                        "season_id": 90,
                        "match_id": 123,
                    },
                    name="Open data match",
                    dataset_type="match",
                    provider="statsbomb",
                    url="http://open-data.com/match123",
                ).add_file(
                    data_feed_key="test",
                    last_modified=datetime.datetime.now(),
                    json_content={"match_id": 123},
                )

    mock_source_class = MockOpenDataSource

    # Replace the real source with mock in the registry
    original_source = loader.OPEN_DATA_SOURCES["statsbomb"]
    loader.OPEN_DATA_SOURCES["statsbomb"] = mock_source_class

    try:
        # No ingestion plans configured - should still work with open data
        datasets = list(
            engine.iter_datasets(
                auto_ingest={"use_open_data": True},
                provider="statsbomb",
                dataset_type="match",
                competition_id=11,
                season_id=90,
            )
        )

        # Should find datasets from auto-discovered StatsBombGithub source
        assert len(datasets) > 0
        assert datasets[0].provider == "statsbomb"
        assert datasets[0].identifier["competition_id"] == 11
    finally:
        # Restore original source
        loader.OPEN_DATA_SOURCES["statsbomb"] = original_source
