import logging
import platform
import uuid
from multiprocessing import set_start_method
from typing import List, Optional

from ingestify.domain.models import Selector
from ingestify.utils import TaskExecutor

from .dataset_store import DatasetStore
from ingestify.domain.models.ingestion.ingestion_plan import IngestionPlan
from ..domain.models.ingestion.ingestion_job import IngestionJob
from ..exceptions import ConfigurationError

if platform.system() == "Darwin":
    set_start_method("fork", force=True)
else:
    set_start_method("spawn", force=True)


logger = logging.getLogger(__name__)


class Loader:
    def __init__(self, store: DatasetStore):
        self.store = store
        self.ingestion_plans: List[IngestionPlan] = []

    def add_ingestion_plan(self, ingestion_plan: IngestionPlan):
        self.ingestion_plans.append(ingestion_plan)

    def collect_and_run(self, dry_run: bool = False, provider: Optional[str] = None):
        # First collect all selectors, before discovering datasets
        selectors = {}
        for ingestion_plan in self.ingestion_plans:
            logger.info(f"Determining selectors for {ingestion_plan}")

            if provider is not None:
                if ingestion_plan.source.provider != provider:
                    logger.info(
                        f"Skipping {ingestion_plan} because provider doesn't match '{provider}'"
                    )
                    continue

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

        """
            Data is denormalized:
            
            It actually looks like:
                - IngestionPlan #1
                    - Selector 1.1
                    - Selector 1.2
                    - Selector 1.3
                - IngestionPlan #2
                    - Selector 2.1
                    - Selector 2.2
                    
            We process this as:
            - IngestionPlan #1, Selector 1.1
            - IngestionPlan #1, Selector 1.2
            - IngestionPlan #1, Selector 1.3
            - IngestionPlan #2, Selector 2.1
            - IngestionPlan #2, Selector 2.2 
            
            IngestionJobSummary holds the summary for an IngestionPlan and a single Selector
        """
        for ingestion_plan, selector in selectors.values():
            logger.info(
                f"Discovering datasets from {ingestion_plan.source.__class__.__name__} using selector {selector}"
            )

            ingestion_job = IngestionJob(
                ingestion_job_id=str(uuid.uuid1()),
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
