from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

from ingestify.domain import Selector
from ingestify.domain.models.execution.extraction_plan import ExtractionPlan
from ingestify.domain.models.task.task_summary import TaskSummary
from ingestify.utils import utcnow


def format_duration(duration: timedelta):
    from humanize import naturaldelta
    return naturaldelta(duration, minimum_unit="MILLISECONDS")


@dataclass
class Timing:
    name: str
    start: datetime
    end: datetime

    @property
    def duration(self):
        return self.end - self.start


@dataclass
class ExtractionJobSummary:
    extraction_plan: ExtractionPlan
    selector: Selector

    start: datetime = field(default_factory=utcnow)
    end: Optional[datetime] = None
    timings: list[Timing] = field(default_factory=list)
    task_summaries: list[TaskSummary] = field(default_factory=list)

    @contextmanager
    def record_timing(self, name: str):
        start = utcnow()
        yield
        self.timings.append(
            Timing(
                name=name,
                start=start,
                end=utcnow()
            )
        )

    def add_task_summaries(self, task_summaries):
        self.task_summaries.extend(task_summaries)

    def set_finished(self):
        self.end = utcnow()

    @property
    def duration(self):
        return self.end - self.start

    def output_report(self):
        print(f"\nExtractionJobSummary finished in {format_duration(self.end - self.start)}")
        print("--------------------")
        print(f"  - ExtractionPlan: {self.extraction_plan}")
        print(f"  - Selector: {self.selector}")
        print(f"  - Timings: ")
        for timing in self.timings:
            print(f"    - {timing.name}: {format_duration(timing.duration)}")
        print(f"  - Tasks: {len(self.task_summaries)} - {(len(self.task_summaries) / self.duration.total_seconds()):.1f} tasks/sec")
        print("--------------------")


