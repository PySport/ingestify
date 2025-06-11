import datetime
from unittest.mock import Mock

from ingestify import DatasetResource
from ingestify.domain.models.dataset.events import SelectorSkipped, DatasetSkipped
from ingestify.domain.models.dataset.dataset import Dataset
from ingestify.domain.models.dataset.dataset_state import DatasetState
from ingestify.domain.models.ingestion.ingestion_job import IngestionJob
from ingestify.domain.models.ingestion.ingestion_plan import IngestionPlan
from ingestify.domain.models.fetch_policy import FetchPolicy
from ingestify.domain import Selector, DataSpecVersionCollection, Identifier, Source
from ingestify.utils import TaskExecutor


class TestIngestionJobEventEmission:
    """Test that IngestionJob emits events at the right times."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_store = Mock()

        # Create a proper mock source that inherits from Source
        class MockTestSource(Source):
            def __init__(self, name):
                super().__init__(name)
                self._find_datasets_mock = Mock(return_value=iter([]))

            @property
            def provider(self):
                return "test_provider"

            def find_datasets(
                self,
                dataset_type,
                data_spec_versions,
                dataset_collection_metadata,
                **kwargs
            ):
                return self._find_datasets_mock(
                    dataset_type,
                    data_spec_versions,
                    dataset_collection_metadata,
                    **kwargs
                )

        self.mock_source = MockTestSource("test_source")

        data_spec_versions = DataSpecVersionCollection.from_dict({"default": {"v1"}})
        self.selector = Selector.build(
            {"competition_id": 11}, data_spec_versions=data_spec_versions
        )

        self.mock_fetch_policy = Mock(spec=FetchPolicy)

        self.ingestion_plan = IngestionPlan(
            source=self.mock_source,
            fetch_policy=self.mock_fetch_policy,
            selectors=[self.selector],
            dataset_type="match",
            data_spec_versions=data_spec_versions,
        )

        self.ingestion_job = IngestionJob(
            ingestion_job_id="test-job",
            ingestion_plan=self.ingestion_plan,
            selector=self.selector,
        )

    def test_selector_skipped_event_emitted_when_up_to_date(self):
        """Test that SelectorSkipped event is emitted when selector is up-to-date."""
        # Setup: selector has last_modified and metadata shows newer data
        self.selector._last_modified = datetime.datetime(2023, 1, 1)

        mock_metadata = Mock()
        mock_metadata.last_modified = datetime.datetime(
            2023, 1, 2
        )  # Newer than selector

        mock_collection = Mock()
        mock_collection.metadata = mock_metadata
        self.mock_store.get_dataset_collection.return_value = mock_collection

        # Execute
        task_executor = TaskExecutor(dry_run=True)
        summaries = list(self.ingestion_job.execute(self.mock_store, task_executor))

        # Verify SelectorSkipped event was dispatched
        self.mock_store.dispatch.assert_called_once()
        dispatched_event = self.mock_store.dispatch.call_args[0][0]
        assert isinstance(dispatched_event, SelectorSkipped)
        assert dispatched_event.selector == self.selector

    def test_dataset_skipped_event_emitted_when_should_refetch_false(self):
        """Test that DatasetSkipped event is emitted when should_refetch returns False."""
        # Setup: selector needs checking (no last_modified)
        self.selector._last_modified = None

        mock_metadata = Mock()
        mock_metadata.last_modified = None
        mock_collection = Mock()
        mock_collection.metadata = mock_metadata

        # Mock dataset exists and should not be refetched
        existing_dataset = Dataset(
            bucket="test",
            dataset_id="existing-id",
            name="Existing Dataset",
            state=DatasetState.COMPLETE,
            identifier=Identifier(competition_id=11, match_id=123),
            dataset_type="match",
            provider="test_provider",
            metadata={},
            created_at=datetime.datetime.now(),
            updated_at=datetime.datetime.now(),
            last_modified_at=None,
        )
        mock_collection.get.return_value = existing_dataset

        self.mock_store.get_dataset_collection.return_value = mock_collection

        # Mock dataset resource from find_datasets
        from ingestify import DatasetResource

        dataset_resource = DatasetResource(
            dataset_resource_id={"competition_id": 11, "match_id": 123},
            name="Test Resource",
            dataset_type="match",
            provider="test_provider",
            url="http://test.com",
        )

        # Mock source returns one dataset resource in a batch
        self.mock_source._find_datasets_mock.return_value = iter([[dataset_resource]])

        # Mock fetch policy says don't refetch
        self.mock_fetch_policy.should_refetch.return_value = False

        # Execute
        task_executor = TaskExecutor(dry_run=True)
        summaries = list(self.ingestion_job.execute(self.mock_store, task_executor))

        # Verify DatasetSkipped event was dispatched
        assert self.mock_store.dispatch.call_count >= 1

        # Find the DatasetSkipped event among the dispatched calls
        dataset_skipped_calls = [
            call
            for call in self.mock_store.dispatch.call_args_list
            if isinstance(call[0][0], DatasetSkipped)
        ]
        assert len(dataset_skipped_calls) == 1

        dispatched_event = dataset_skipped_calls[0][0][0]
        assert dispatched_event.dataset == existing_dataset

    def test_no_events_emitted_when_tasks_created(self):
        """Test that no skipping events are emitted when actual tasks are created and executed."""
        # Setup: selector needs checking and dataset should be refetched
        self.selector._last_modified = None

        mock_metadata = Mock()
        mock_metadata.last_modified = None
        mock_collection = Mock()
        mock_collection.metadata = mock_metadata
        mock_collection.get.return_value = None  # No existing dataset

        self.mock_store.get_dataset_collection.return_value = mock_collection

        # Mock dataset resource from find_datasets
        dataset_resource = DatasetResource(
            dataset_resource_id={"competition_id": 11, "match_id": 123},
            name="Test Resource",
            dataset_type="match",
            provider="test_provider",
            url="http://test.com",
        )

        self.mock_source._find_datasets_mock.return_value = iter([[dataset_resource]])
        self.mock_fetch_policy.should_fetch.return_value = True

        # Execute with a simple task executor that doesn't fail on None tasks
        task_executor = TaskExecutor(dry_run=True)

        # Mock the task executor to simulate task execution
        mock_task_summary = Mock()
        mock_executor = Mock()
        mock_executor.map.return_value = [mock_task_summary]
        task_executor.executor = mock_executor

        summaries = list(self.ingestion_job.execute(self.mock_store, task_executor))

        # Verify tasks were executed (mock executor was called)
        assert mock_executor.map.called

        # Verify no skipping events were dispatched (tasks should be created and executed instead)
        skipping_event_calls = [
            call
            for call in self.mock_store.dispatch.call_args_list
            if isinstance(call[0][0], (SelectorSkipped, DatasetSkipped))
        ]
        assert len(skipping_event_calls) == 0
