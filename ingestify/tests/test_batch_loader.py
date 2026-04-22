"""Tests for BatchLoader."""
import pytest

from ingestify.domain.models.resources.batch_loader import BatchLoader


def test_batch_loader_returns_cached_result():
    """__call__ returns the result stored for each file_resource."""
    batch_loader = BatchLoader(lambda frs, cfs, drs: [], batch_size=5)
    fr1, fr2 = object(), object()

    batch_loader._store_results([fr1, fr2], ["result_1", "result_2"])

    assert batch_loader(fr1, current_file=None) == "result_1"
    assert batch_loader(fr2, current_file=None) == "result_2"


def test_batch_loader_raises_if_not_precomputed():
    """__call__ raises when the cache has no entry for this file_resource."""
    batch_loader = BatchLoader(lambda frs, cfs, drs: [], batch_size=5)

    with pytest.raises(RuntimeError, match="not precomputed"):
        batch_loader(object(), current_file=None)


def test_batch_loader_propagates_stored_exception():
    """When an Exception is stored as a result, __call__ re-raises it."""
    batch_loader = BatchLoader(lambda frs, cfs, drs: [], batch_size=5)
    fr_ok, fr_err = object(), object()

    original_error = ValueError("Google Ads daily quota exhausted")
    batch_loader._store_results([fr_ok, fr_err], ["good_result", original_error])

    # Good result works normally
    assert batch_loader(fr_ok, current_file=None) == "good_result"

    # Error result re-raises the original exception
    with pytest.raises(ValueError, match="daily quota exhausted"):
        batch_loader(fr_err, current_file=None)
