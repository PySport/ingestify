import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, TYPE_CHECKING, Literal

from ingestify.domain.models.timing import Timing

if TYPE_CHECKING:
    from ingestify.domain import Revision, DraftFile

from ingestify.utils import utcnow


logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    # SCHEDULED = "SCHEDULED"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"


class Operation(Enum):
    CREATE = "CREATE"
    UPDATE = "UPDATE"


@dataclass
class TaskSummary:
    start: datetime
    operation: Operation
    end: Optional[datetime] = None
    persisted_file_count: int = 0
    bytes_retrieved: int = 0
    last_modified: Optional[datetime] = None
    status: TaskStatus = TaskStatus.RUNNING

    timings: list[Timing] = field(default_factory=list)

    def record_load_file(self, fn, metadata: dict):
        start = utcnow()
        try:
            result = None
            return fn()
        except Exception as e:
            result = e
            raise e
        finally:
            metadata = dict(result=result, **metadata)
            self.timings.append(
                Timing(
                    name=f"Load of {metadata.get('file_id', 'file')}",
                    start=start,
                    end=utcnow(),
                    metadata=metadata,
                )
            )

    @classmethod
    @contextmanager
    def new(cls, operation):
        start = utcnow()
        task_summary = cls(start=start, operation=operation)
        try:
            yield task_summary

            task_summary.status = TaskStatus.FINISHED
        except Exception as e:
            logger.exception(f"Failed to execute task.")
            task_summary.status = TaskStatus.FAILED
        finally:
            task_summary.end = utcnow()

    @classmethod
    def update(cls):
        return cls.new("update")

    @classmethod
    def create(cls):
        return cls.new("create")

    def set_stats_from_revision(self, revision: Optional["Revision"]):
        if revision:
            self.persisted_file_count = len(revision.modified_files)
            self.bytes_retrieved = sum(file.size for file in revision.modified_files)
            self.last_modified = max(
                file.modified_at for file in revision.modified_files
            )
