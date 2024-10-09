from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from ingestify.domain import Revision

from ingestify.utils import utcnow


@dataclass
class TaskSummary:
    start: datetime
    operation: Literal["create", "update"]
    end: Optional[datetime] = None
    persisted_file_count: int = 0
    bytes_retrieved: int = 0
    last_modified: Optional[datetime] = None

    @classmethod
    @contextmanager
    def new(cls, operation):
        start = utcnow()
        task_summary = cls(
            start=start,
            operation=operation
        )
        try:
            yield task_summary
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
            self.bytes_retrieved = sum(
                file.size for file in revision.modified_files
            )
            self.last_modified = max(
                file.modified_at for file in revision.modified_files
            )





