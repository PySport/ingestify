import logging
import platform
import uuid
from multiprocessing import set_start_method
from typing import List, Optional

from ingestify.domain.models import Selector
from ingestify.utils import TaskExecutor

from .dataset_store import DatasetStore
from ingestify.domain.models.ingestion.ingestion_plan import IngestionPlan
from ingestify.domain.models.fetch_policy import FetchPolicy
from ingestify.domain import DataSpecVersionCollection
from ingestify.infra.source.statsbomb_github import StatsbombGithub
from ..domain.models.ingestion.ingestion_job import IngestionJob
from ..exceptions import ConfigurationError

if platform.system() == "Darwin":
    set_start_method("fork", force=True)
else:
    set_start_method("spawn", force=True)


logger = logging.getLogger(__name__)


# Registry of open data sources that can be auto-instantiated
OPEN_DATA_SOURCES = {
    "statsbomb": StatsbombGithub,
    # Add more open data sources here as they become available
}


def _create_open_data_plan(provider: str, dataset_type: str) -> Optional[IngestionPlan]:
    """Create a temporary ingestion plan for open data sources."""
    if provider not in OPEN_DATA_SOURCES:
        return None

    source_class = OPEN_DATA_SOURCES[provider]
    source = source_class(name=f"open_data_{provider}")

    # Create empty selector to trigger discover_selectors
    data_spec_versions = DataSpecVersionCollection.from_dict({"default": {"v1"}})
    empty_selector = Selector.build({}, data_spec_versions=data_spec_versions)

    return IngestionPlan(
        source=source,
        fetch_policy=FetchPolicy(),
        selectors=[empty_selector],
        dataset_type=dataset_type,
        data_spec_versions=data_spec_versions,
    )


class Loader:
    def __init__(self, store: DatasetStore):
        self.store = store
        self.ingestion_plans: List[IngestionPlan] = []

    def add_ingestion_plan(self, ingestion_plan: IngestionPlan):
        self.ingestion_plans.append(ingestion_plan)

    def collect(
        self,
        provider: Optional[str] = None,
        source: Optional[str] = None,
        dataset_type: Optional[str] = None,
        auto_ingest_config: Optional[dict] = None,
        **selector_filters,
    ):
        """Collect and prepare selectors for execution."""
        ingestion_plans = []
        for ingestion_plan in self.ingestion_plans:
            if provider is not None:
                if ingestion_plan.source.provider != provider:
                    logger.debug(
                        f"Skipping {ingestion_plan} because provider doesn't match '{provider}'"
                    )
                    continue

            if source is not None:
                if ingestion_plan.source.name != source:
                    logger.debug(
                        f"Skipping {ingestion_plan} because source doesn't match '{source}'"
                    )
                    continue

            if dataset_type is not None:
                if ingestion_plan.dataset_type != dataset_type:
                    logger.debug(
                        f"Skipping {ingestion_plan} because dataset_type doesn't match '{dataset_type}'"
                    )
                    continue

            # Note: Selector filtering is now done after all selectors are collected
            # to allow discover_selectors to run for plans with empty selectors

            ingestion_plans.append(ingestion_plan)

        # Check if we need to add open data plans
        auto_ingest_config = auto_ingest_config or {}
        if auto_ingest_config.get("use_open_data", False):
            # Validate prerequisites for open data
            if not provider:
                raise ConfigurationError(
                    "use_open_data requires 'provider' to be specified"
                )
            if not dataset_type:
                raise ConfigurationError(
                    "use_open_data requires 'dataset_type' to be specified"
                )

            # Only add open data plan if no matching configured plans found
            if not ingestion_plans:
                open_data_plan = _create_open_data_plan(provider, dataset_type)
                if open_data_plan:
                    logger.info(f"Auto-discovered open data source: {open_data_plan}")
                    ingestion_plans.append(open_data_plan)
                else:
                    logger.warning(
                        f"No open data source available for provider '{provider}'"
                    )

        # First collect all selectors, before discovering datasets
        selectors = {}
        for ingestion_plan in ingestion_plans:
            logger.info(f"Determining selectors for {ingestion_plan}")

            static_selectors = [
                selector
                for selector in ingestion_plan.selectors
                if not selector.is_dynamic
            ]
            dynamic_selectors = [
                selector for selector in ingestion_plan.selectors if selector.is_dynamic
            ]

            no_selectors = len(static_selectors) == 1 and not bool(static_selectors[0])
            if dynamic_selectors or no_selectors:
                if hasattr(ingestion_plan.source, "discover_selectors"):
                    logger.debug(
                        f"Discovering selectors from {ingestion_plan.source.__class__.__name__}"
                    )

                    # TODO: consider making this lazy and fetch once per Source instead of
                    #       once per IngestionPlan
                    # TODO: Log exception when `discover_selectors` fails
                    all_selectors = ingestion_plan.source.discover_selectors(
                        ingestion_plan.dataset_type
                    )
                    if no_selectors:
                        # When there were no selectors specified, just use all of them
                        extra_static_selectors = [
                            Selector.build(
                                job_selector,
                                data_spec_versions=ingestion_plan.data_spec_versions,
                            )
                            for job_selector in all_selectors
                        ]
                        static_selectors = []
                    else:
                        extra_static_selectors = []
                        for dynamic_selector in dynamic_selectors:
                            dynamic_job_selectors = [
                                Selector.build(
                                    job_selector,
                                    data_spec_versions=ingestion_plan.data_spec_versions,
                                )
                                for job_selector in all_selectors
                                if dynamic_selector.is_match(job_selector)
                            ]
                            extra_static_selectors.extend(dynamic_job_selectors)
                            logger.info(f"Added {len(dynamic_job_selectors)} selectors")

                    static_selectors.extend(extra_static_selectors)

                    logger.info(
                        f"Discovered {len(extra_static_selectors)} selectors from {ingestion_plan.source.__class__.__name__}"
                    )
                else:
                    if not no_selectors:
                        # When there are no selectors and no discover_selectors, just pass it through. It might break
                        # later on
                        raise ConfigurationError(
                            f"Dynamic selectors cannot be used for "
                            f"{ingestion_plan.source.__class__.__name__} because it doesn't support"
                            f" selector discovery"
                        )

            # Merge selectors when source, dataset_type and actual selector is the same. This makes
            # sure there will be only 1 dataset for this combination
            for selector in static_selectors:
                key = (
                    ingestion_plan.source.name,
                    ingestion_plan.dataset_type,
                    selector.key,
                )
                if existing_selector := selectors.get(key):
                    existing_selector[1].data_spec_versions.merge(
                        selector.data_spec_versions
                    )
                else:
                    selectors[key] = (ingestion_plan, selector)

        # Convert to list
        collected_selectors = list(selectors.values())

        # Apply selector filters if provided
        if selector_filters:
            filtered_selectors = []
            for ingestion_plan, selector in collected_selectors:
                if selector.matches(selector_filters):
                    # Merge selector with user filters to make it more strict
                    merged_attributes = {
                        **selector.filtered_attributes,
                        **selector_filters,
                    }
                    strict_selector = Selector.build(
                        merged_attributes,
                        data_spec_versions=selector.data_spec_versions,
                    )

                    # Check if selector was actually made more strict
                    if len(strict_selector.filtered_attributes) > len(
                        selector.filtered_attributes
                    ):
                        logger.debug(
                            f"Made selector more strict: {selector} -> {strict_selector}"
                        )

                    filtered_selectors.append((ingestion_plan, strict_selector))
                else:
                    logger.debug(
                        f"Filtering out selector {selector} because it doesn't match filters"
                    )
            collected_selectors = filtered_selectors

        return collected_selectors

    def run(self, selectors, dry_run: bool = False):
        """Execute the collected selectors."""
        ingestion_job_prefix = str(uuid.uuid1())
        for ingestion_job_idx, (ingestion_plan, selector) in enumerate(selectors):
            logger.info(
                f"Discovering datasets from {ingestion_plan.source.__class__.__name__} using selector {selector}"
            )

            ingestion_job = IngestionJob(
                # Create a combined IngestionJobId.
                # This allows us to group all IngestionJobs within the same run
                ingestion_job_id=f"{ingestion_job_prefix}.{ingestion_job_idx}",
                ingestion_plan=ingestion_plan,
                selector=selector,
            )

            with TaskExecutor(dry_run=dry_run) as task_executor:
                for ingestion_job_summary in ingestion_job.execute(
                    self.store, task_executor=task_executor
                ):
                    # TODO: handle task_summaries
                    #       Summarize to a IngestionJobSummary, and save to a database. This Summary can later be used in a
                    #       next run to determine where to resume.
                    # TODO 2: Do we want to add additional information from the summary back to the Task, so it can use
                    #      extra information to determine how/where to resume
                    ingestion_job_summary.output_report()
                    logger.info(f"Storing IngestionJobSummary")
                    self.store.save_ingestion_job_summary(ingestion_job_summary)

        logger.info("Done")

    def collect_and_run(
        self,
        dry_run: bool = False,
        provider: Optional[str] = None,
        source: Optional[str] = None,
        dataset_type: Optional[str] = None,
        auto_ingest_config: Optional[dict] = None,
        **selector_filters,
    ):
        """
        Backward compatibility method - collect then run.

        Data flow explanation:

        IngestionPlans are structured hierarchically:
            - IngestionPlan #1
                - Selector 1.1
                - Selector 1.2
                - Selector 1.3
            - IngestionPlan #2
                - Selector 2.1
                - Selector 2.2

        But we process them as flat (plan, selector) pairs for execution:
            - (IngestionPlan #1, Selector 1.1)
            - (IngestionPlan #1, Selector 1.2)
            - (IngestionPlan #1, Selector 1.3)
            - (IngestionPlan #2, Selector 2.1)
            - (IngestionPlan #2, Selector 2.2)

        Each IngestionJobSummary tracks the execution of one (IngestionPlan, Selector) pair.
        """
        selectors = self.collect(
            provider=provider,
            source=source,
            dataset_type=dataset_type,
            auto_ingest_config=auto_ingest_config,
            **selector_filters,
        )
        if selector_filters and not selectors:
            logger.warning(f"No data found matching {selector_filters}")
        else:
            self.run(selectors, dry_run=dry_run)
