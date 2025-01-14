from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Optional, List, TYPE_CHECKING
from pydantic import Field

from ingestify.domain import Selector, DataSpecVersionCollection
from ingestify.domain.models.base import BaseModel
from ingestify.domain.models.task.task_summary import TaskSummary, TaskStatus
from ingestify.domain.models.timing import Timing
from ingestify.utils import utcnow

if TYPE_CHECKING:
    from ingestify.domain.models.ingestion.ingestion_job import IngestionJob


def format_duration(duration: timedelta):
    from humanize import naturaldelta

    return naturaldelta(duration, minimum_unit="MILLISECONDS")


class IngestionJobSummary(BaseModel):
    ingestion_job_id: str

    # From the IngestionPlan
    source_name: str
    dataset_type: str
    data_spec_versions: DataSpecVersionCollection
    selector: Selector

    started_at: datetime = Field(default_factory=utcnow)
    finished_at: Optional[datetime] = None
    timings: List[Timing] = Field(default_factory=list)
    task_summaries: List[TaskSummary] = Field(default_factory=list)

    failed_tasks: int = 0
    successful_tasks: int = 0
    ignored_successful_tasks: int = 0

    @classmethod
    def new(cls, ingestion_job: "IngestionJob"):
        args = dict(
            ingestion_job_id=ingestion_job.ingestion_job_id,
            source_name=ingestion_job.ingestion_plan.source.name,
            dataset_type=ingestion_job.ingestion_plan.dataset_type,
            data_spec_versions=ingestion_job.ingestion_plan.data_spec_versions,
            selector=ingestion_job.selector,
        )
        return cls(**args)

    @contextmanager
    def record_timing(self, name: str):
        start = utcnow()
        yield
        self.timings.append(Timing(name=name, started_at=start, ended_at=utcnow()))

    def add_task_summaries(self, task_summaries: List[TaskSummary]):
        self.task_summaries.extend(task_summaries)

    def set_finished(self):
        self.failed_tasks = len(
            [task for task in self.task_summaries if task.status == TaskStatus.FAILED]
        )
        self.successful_tasks = len(
            [task for task in self.task_summaries if task.status == TaskStatus.FINISHED]
        )
        self.ignored_successful_tasks = len(
            [
                task
                for task in self.task_summaries
                if task.status == TaskStatus.FINISHED_IGNORED
            ]
        )
        self.finished_at = utcnow()

    @property
    def duration(self) -> timedelta:
        return self.finished_at - self.started_at

    def output_report(self):
        print(f"\nIngestionJobSummary finished in {format_duration(self.duration)}")
        print("--------------------")
        print(f"  - IngestionPlan:")
        print(f"        Source: {self.source_name}")
        print(f"        DatasetType: {self.dataset_type}")
        print(f"  - Selector: {self.selector}")
        print(f"  - Timings: ")
        for timing in self.timings:
            print(f"    - {timing.name}: {format_duration(timing.duration)}")
        print(
            f"  - Tasks: {len(self.task_summaries)} - {(len(self.task_summaries) / self.duration.total_seconds()):.1f} tasks/sec"
        )

        for status in [
            TaskStatus.FAILED,
            TaskStatus.FINISHED,
            TaskStatus.FINISHED_IGNORED,
        ]:
            print(
                f"    - {status.value.lower()}: {len([task for task in self.task_summaries if task.status == status])}"
            )
        print("--------------------")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
