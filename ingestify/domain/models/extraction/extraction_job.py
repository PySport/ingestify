from ingestify.application.dataset_store import DatasetStore
from ingestify.domain.models.extraction.extraction_job_summary import ExtractionJobSummary
from ingestify.utils import TaskExecutor


class ExtractionJob:
    def __init__(self, extraction_plan: ExtractionPlan, selector: Selector):
        self.extraction_plan = extraction_plan
        self.selector = selector

    def execute(self, store: DatasetStore, task_executor: TaskExecutor):
        extraction_job_summary = ExtractionJobSummary(
            extraction_plan=self.extraction_plan,
            selector=self.selector
        )

        with extraction_job_summary.record_timing("get_dataset_collection"):
            dataset_collection_metadata = store.get_dataset_collection(
                dataset_type=self.extraction_plan.dataset_type,
                data_spec_versions=self.selector.data_spec_versions,
                selector=self.selector,
                metadata_only=True,
            ).metadata

        # There are two different, but similar flows here:
        # 1. The discover_datasets returns a list, and the entire list can be processed at once
        # 2. The discover_datasets returns an iterator of batches, in this case we need to process each batch
        with extraction_job_summary.record_timing("find_datasets"):
            # Timing might be incorrect as it is an iterator
            datasets = self.extraction_plan.source.find_datasets(
                dataset_type=self.extraction_plan.dataset_type,
                data_spec_versions=self.selector.data_spec_versions,
                dataset_collection_metadata=dataset_collection_metadata,
                **self.selector.custom_attributes,
            )

