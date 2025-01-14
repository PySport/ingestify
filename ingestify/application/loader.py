import logging
import platform
import uuid
from multiprocessing import set_start_method
from typing import List, Optional

from ingestify.domain.models import Selector
from ingestify.utils import TaskExecutor

from .dataset_store import DatasetStore
from ingestify.domain.models.extraction.extraction_plan import ExtractionPlan
from ..domain.models.extraction.extraction_job import ExtractionJob
from ..exceptions import ConfigurationError

if platform.system() == "Darwin":
    set_start_method("fork", force=True)
else:
    set_start_method("spawn", force=True)


logger = logging.getLogger(__name__)


class Loader:
    def __init__(self, store: DatasetStore):
        self.store = store
        self.extraction_plans: List[ExtractionPlan] = []

    def add_extraction_plan(self, extraction_plan: ExtractionPlan):
        self.extraction_plans.append(extraction_plan)

    def collect_and_run(self, dry_run: bool = False, provider: Optional[str] = None):
        # First collect all selectors, before discovering datasets
        selectors = {}
        for extraction_plan in self.extraction_plans:
            if provider is not None:
                if extraction_plan.source.provider != provider:
                    logger.info(
                        f"Skipping {extraction_plan} because provider doesn't match '{provider}'"
                    )
                    continue

            static_selectors = [
                selector
                for selector in extraction_plan.selectors
                if not selector.is_dynamic
            ]
            dynamic_selectors = [
                selector
                for selector in extraction_plan.selectors
                if selector.is_dynamic
            ]

            no_selectors = len(static_selectors) == 1 and not bool(static_selectors[0])
            if dynamic_selectors or no_selectors:
                if hasattr(extraction_plan.source, "discover_selectors"):
                    logger.debug(
                        f"Discovering selectors from {extraction_plan.source.__class__.__name__}"
                    )

                    # TODO: consider making this lazy and fetch once per Source instead of
                    #       once per ExtractionPlan
                    all_selectors = extraction_plan.source.discover_selectors(
                        extraction_plan.dataset_type
                    )
                    if no_selectors:
                        # When there were no selectors specified, just use all of them
                        extra_static_selectors = [
                            Selector.build(
                                job_selector,
                                data_spec_versions=extraction_plan.data_spec_versions,
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
                                    data_spec_versions=extraction_plan.data_spec_versions,
                                )
                                for job_selector in all_selectors
                                if dynamic_selector.is_match(job_selector)
                            ]
                            extra_static_selectors.extend(dynamic_job_selectors)
                            logger.info(f"Added {len(dynamic_job_selectors)} selectors")

                    static_selectors.extend(extra_static_selectors)

                    logger.info(
                        f"Discovered {len(extra_static_selectors)} selectors from {extraction_plan.source.__class__.__name__}"
                    )
                else:
                    if not no_selectors:
                        # When there are no selectors and no discover_selectors, just pass it through. It might break
                        # later on
                        raise ConfigurationError(
                            f"Dynamic selectors cannot be used for "
                            f"{extraction_plan.source.__class__.__name__} because it doesn't support"
                            f" selector discovery"
                        )

            # Merge selectors when source, dataset_type and actual selector is the same. This makes
            # sure there will be only 1 dataset for this combination
            for selector in static_selectors:
                key = (
                    extraction_plan.source.name,
                    extraction_plan.dataset_type,
                    selector.key,
                )
                if existing_selector := selectors.get(key):
                    existing_selector[1].data_spec_versions.merge(
                        selector.data_spec_versions
                    )
                else:
                    selectors[key] = (extraction_plan, selector)

        """
            Data is denormalized:
            
            It actually looks like:
                - ExtractionPlan #1
                    - Selector 1.1
                    - Selector 1.2
                    - Selector 1.3
                - ExtractionPlan #2
                    - Selector 2.1
                    - Selector 2.2
                    
            We process this as:
            - ExtractionPlan #1, Selector 1.1
            - ExtractionPlan #1, Selector 1.2
            - ExtractionPlan #1, Selector 1.3
            - ExtractionPlan #2, Selector 2.1
            - ExtractionPlan #2, Selector 2.2 
            
            ExtractionJobSummary holds the summary for an ExtractionPlan and a single Selector
        """
        for extraction_plan, selector in selectors.values():
            logger.debug(
                f"Discovering datasets from {extraction_plan.source.__class__.__name__} using selector {selector}"
            )

            extraction_job = ExtractionJob(
                extraction_job_id=str(uuid.uuid1()),
                extraction_plan=extraction_plan,
                selector=selector,
            )

            with TaskExecutor(dry_run=dry_run) as task_executor:
                extraction_job_summary = extraction_job.execute(
                    self.store, task_executor=task_executor
                )

                # TODO: handle task_summaries
                #       Summarize to a ExtractionJobSummary, and save to a database. This Summary can later be used in a
                #       next run to determine where to resume.
                # TODO 2: Do we want to add additional information from the summary back to the Task, so it can use
                #      extra information to determine how/where to resume
                extraction_job_summary.set_finished()

            extraction_job_summary.output_report()
            self.store.save_extraction_job_summary(extraction_job_summary)

        logger.info("Done")
