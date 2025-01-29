import logging
import traceback
from contextlib import contextmanager
from datetime import datetime
from enum import Enum
from typing import Optional, List
from pydantic import Field, field_validator

from ingestify.domain.models.base import BaseModel
from ingestify.domain.models.dataset.identifier import Identifier
from ingestify.domain.models.timing import Timing
from ingestify.exceptions import IngestifyError
from ingestify.utils import utcnow


logger = logging.getLogger(__name__)


class TaskState(str, Enum):
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    FINISHED_IGNORED = "FINISHED_IGNORED"  # Finished, but didn't produce any new data
    FAILED = "FAILED"


class Operation(str, Enum):
    CREATE = "CREATE"
    UPDATE = "UPDATE"


class TaskSummary(BaseModel):
    task_id: str
    started_at: datetime
    operation: Operation
    dataset_identifier: Identifier
    ended_at: Optional[datetime] = None
    persisted_file_count: int = 0
    bytes_retrieved: int = 0
    last_modified: Optional[datetime] = None
    state: TaskState = TaskState.RUNNING
    timings: List[Timing] = Field(default_factory=list)

    @field_validator("dataset_identifier", mode="before")
    @classmethod
    def ensure_list(cls, value) -> Identifier:
        if not isinstance(value, Identifier):
            return Identifier(**value)
        return value

    def record_load_file(self, fn, metadata: dict):
        start = utcnow()
        try:
            result = None
            return fn()
        except Exception as e:
            result = {
                "type": type(e).__name__,
                "message": str(e),
                "traceback": traceback.format_exc(),
            }
            raise e
        finally:
            metadata = dict(result=result, **metadata)
            self.timings.append(
                Timing(
                    name=f"Load of {metadata.get('file_id', 'file')}",
                    started_at=start,
                    ended_at=utcnow(),
                    metadata=metadata,
                )
            )

    @classmethod
    @contextmanager
    def new(cls, task_id: str, operation: Operation, dataset_identifier: Identifier):
        start = utcnow()
        task_summary = cls(
            task_id=task_id,
            started_at=start,
            operation=operation,
            dataset_identifier=dataset_identifier,
        )
        try:
            yield task_summary

            task_summary.set_state(TaskState.FINISHED)
        except Exception as e:
            logger.exception(f"Failed to execute task.")
            task_summary.set_state(TaskState.FAILED)

            # When the error comes from our own code, make sure it will be raised to the highest level
            # raise
            if isinstance(e, IngestifyError):
                raise
        finally:
            task_summary.ended_at = utcnow()

    @classmethod
    def update(cls, task_id: str, dataset_identifier: Identifier):
        return cls.new(task_id, Operation.UPDATE, dataset_identifier)

    @classmethod
    def create(cls, task_id: str, dataset_identifier: Identifier):
        return cls.new(task_id, Operation.CREATE, dataset_identifier)

    def set_stats_from_revision(self, revision: Optional["Revision"]):
        if revision:
            self.persisted_file_count = len(revision.modified_files)
            self.bytes_retrieved = sum(file.size for file in revision.modified_files)
            self.last_modified = max(
                file.modified_at for file in revision.modified_files
            )
        else:
            self.state = TaskState.FINISHED_IGNORED

    def set_state(self, state: TaskState):
        if self.state == TaskState.RUNNING:
            self.state = state
