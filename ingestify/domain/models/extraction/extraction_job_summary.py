from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, TYPE_CHECKING

from ingestify.domain import Selector, DataSpecVersionCollection
from ingestify.domain.models.extraction.extraction_plan import ExtractionPlan

if TYPE_CHECKING:
    from ingestify.domain.models.extraction.extraction_job import ExtractionJob

from ingestify.domain.models.task.task_summary import TaskSummary, TaskStatus
from ingestify.domain.models.timing import Timing
from ingestify.utils import utcnow


def format_duration(duration: timedelta):
    from humanize import naturaldelta

    return naturaldelta(duration, minimum_unit="MILLISECONDS")


@dataclass
class ExtractionJobSummary:
    extraction_job_id: str

    # From the ExtractionPlan
    source_name: str
    dataset_type: str
    # data_spec_versions: DataSpecVersionCollection
    selector: Selector

    started_at: datetime = field(default_factory=utcnow)
    finished_at: Optional[datetime] = None
    timings: list[Timing] = field(default_factory=list)
    task_summaries: list[TaskSummary] = field(default_factory=list)

    failed_tasks: int = 0
    successful_tasks: int = 0

    @classmethod
    def new(cls, extraction_job: "ExtractionJob"):
        return cls(
            extraction_job_id=extraction_job.extraction_job_id,

            source_name=extraction_job.extraction_plan.source.name,
            dataset_type=extraction_job.extraction_plan.dataset_type,
            # data_spec_versions=extraction_job.extraction_plan.data_spec_versions,
            selector=extraction_job.selector

        )

    @contextmanager
    def record_timing(self, name: str):
        start = utcnow()
        yield
        self.timings.append(Timing(name=name, start=start, end=utcnow()))

    def add_task_summaries(self, task_summaries: list[TaskSummary]):
        self.task_summaries.extend(task_summaries)

    def set_finished(self):
        self.finished_at = utcnow()
        # for task_summary in self.task_summaries:

    @property
    def duration(self):
        return self.finished_at - self.started_at

    def output_report(self):
        print(
            f"\nExtractionJobSummary finished in {format_duration(self.duration)}"
        )
        print("--------------------")
        print(f"  - ExtractionPlan:")
        print(f"        Source: {self.source_name}")
        print(f"        DatasetType: {self.dataset_type}")
        print(f"  - Selector: {self.selector}")
        print(f"  - Timings: ")
        for timing in self.timings:
            print(f"    - {timing.name}: {format_duration(timing.duration)}")
        print(
            f"  - Tasks: {len(self.task_summaries)} - {(len(self.task_summaries) / self.duration.total_seconds()):.1f} tasks/sec"
        )

        for status in [TaskStatus.FAILED, TaskStatus.FINISHED]:
            print(
                f"    - {status.value.lower()}: {len([task for task in self.task_summaries if task.status == status])}"
            )
        print("--------------------")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
