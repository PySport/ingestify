from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, List, Union

from .collection import DatasetCollection
from .dataset import Dataset, DatasetSummaryMap
from .dataset_state import DatasetState
from .selector import Selector


class RunLock:
    """Handle for a held single-run lock (one per job identity). ``release()`` frees it;
    it is also released automatically when the owning DB connection/process ends."""

    def release(self) -> None:
        pass


class NoopRunLock(RunLock):
    """Always-granted lock for stores without cross-process locking (e.g. SQLite)."""


class DatasetRepository(ABC):
    def acquire_run_lock(self, job_key: str) -> Optional[RunLock]:
        """Best-effort single-run lock for one job identity (design:
        docs/design/single-run-lock.md). Return a held ``RunLock``, or ``None`` if another
        process already holds it. Stores without cross-process locking return an
        always-granted no-op lock, so a lone local process is never blocked."""
        return NoopRunLock()

    @abstractmethod
    def get_dataset_collection(
        self,
        bucket: str,
        dataset_type: Optional[str] = None,
        dataset_id: Optional[Union[str, List[str]]] = None,
        provider: Optional[str] = None,
        selector: Optional[Union[Selector, List[Selector]]] = None,
        metadata_only: bool = False,
        dataset_state: Optional[List[DatasetState]] = None,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
    ) -> DatasetCollection:
        pass

    def get_dataset_summary_map(
        self,
        bucket: str,
        provider: str,
        dataset_type: str,
    ) -> DatasetSummaryMap:
        """Return {identifier_json: DatasetSummary} for all datasets matching the
        given provider and dataset_type. Feeds FetchPolicy.can_skip as a cheap
        pre-check, so an up-to-date dataset is skipped without loading the full
        dataset+revision+file graph. Each summary reflects the latest revision."""
        return {}

    def invalidate_revision(self, dataset: Dataset):
        """Mark the current revision as VALIDATION_FAILED and reset
        last_modified_at on the dataset."""
        self.invalidate_revisions([dataset])

    @abstractmethod
    def invalidate_revisions(self, datasets: list[Dataset]):
        """Batch invalidate: mark current revisions as VALIDATION_FAILED
        and reset last_modified_at on the datasets."""
        pass

    @abstractmethod
    def destroy(self, dataset: Dataset):
        pass

    @abstractmethod
    def save(self, bucket: str, dataset: Dataset):
        pass

    @abstractmethod
    def next_identity(self):
        pass
