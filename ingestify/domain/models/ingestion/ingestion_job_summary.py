import uuid
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
    return f"{duration.total_seconds():.2f}sec"


class IngestionJobSummary(BaseModel):
    ingestion_job_summary_id: str
    ingestion_job_id: str

    # From the IngestionPlan
    provider: str
    source_name: str
    dataset_type: str
    data_spec_versions: DataSpecVersionCollection
    selector: Selector

    started_at: datetime = Field(default_factory=utcnow)
    finished_at: Optional[datetime] = None
    timings: List[Timing] = Field(default_factory=list)
    task_summaries: List[TaskSummary] = Field(default_factory=list)

    skipped_datasets: int = 0
    failed_tasks: int = 0
    successful_tasks: int = 0
    ignored_successful_tasks: int = 0

    @classmethod
    def new(cls, ingestion_job: "IngestionJob"):
        args = dict(
            ingestion_job_summary_id=str(uuid.uuid1()),
            ingestion_job_id=ingestion_job.ingestion_job_id,
            provider=ingestion_job.ingestion_plan.source.provider,
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

    def start_timing(self, name):
        start = utcnow()

        def finish():
            self.timings.append(Timing(name=name, started_at=start, ended_at=utcnow()))

        return finish

    def add_task_summaries(self, task_summaries: List[TaskSummary]):
        self.task_summaries.extend(task_summaries)

    def increase_skipped_datasets(self, skipped_datasets: int):
        self.skipped_datasets += skipped_datasets

    def task_count(self):
        return len(self.task_summaries)

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
        print(f"        Provider: {self.provider}")
        print(f"        DatasetType: {self.dataset_type}")
        print(f"  - Selector: {self.selector}")
        print(f"  - Timings: ")
        for timing in self.timings:
            print(f"    - {timing.name}: {format_duration(timing.duration)}")
        print(
            f"  - Tasks: {len(self.task_summaries)} - {(len(self.task_summaries) / self.duration.total_seconds()):.1f} tasks/sec"
        )

        print(f"    - Failed tasks: {self.failed_tasks}")
        print(f"    - Successful tasks: {self.successful_tasks}")
        print(f"    - Successful ignored tasks: {self.successful_tasks}")
        print(f"    - Skipped datasets: {self.skipped_datasets}")
        print("--------------------")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
