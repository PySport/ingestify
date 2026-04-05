"""BatchLoader wraps a file loader so multiple files are fetched in one call.

Use this when the data source is more efficient with batched requests. Create
one BatchLoader instance and share it across the DatasetResources that should
be batched together. Ingestify groups those resources by shared BatchLoader
instance, chunks them into groups of `batch_size`, and calls the wrapped
loader_fn once per chunk.

The wrapped loader_fn receives lists instead of single items and must return
a list of results in the same order:

    def load(file_resources, current_files, dataset_resources):
        ...
        return [DraftFile.from_input(...) for _ in file_resources]

    batch_loader = BatchLoader(load, batch_size=20)
    resource.add_file(file_loader=batch_loader, ...)

current_files may contain None entries (for create tasks) or a File (for
update tasks) — the loader_fn handles both.
"""
import threading
from typing import Callable, List


class BatchLoader:
    def __init__(self, loader_fn: Callable, batch_size: int):
        self.loader_fn = loader_fn
        self.batch_size = batch_size
        self._results: dict = {}
        self._lock = threading.Lock()

    def __call__(self, file_resource, current_file, dataset_resource=None, **kwargs):
        with self._lock:
            if id(file_resource) not in self._results:
                raise RuntimeError(
                    "BatchLoader result not precomputed. A BatchTask must "
                    "populate the cache before inner tasks execute."
                )
            return self._results.pop(id(file_resource))

    def _store_results(self, file_resources: List, results: List):
        """Store batch results so they can be retrieved via __call__."""
        with self._lock:
            for file_resource, result in zip(file_resources, results):
                self._results[id(file_resource)] = result
