class IngestifyError(Exception):
    pass


class ConfigurationError(IngestifyError):
    pass


class DuplicateFile(IngestifyError):
    pass


class SaveError(IngestifyError):
    pass


class StopProcessing(IngestifyError):
    """Raised by a source or loader to signal that processing should stop
    gracefully. Successfully processed datasets are preserved; the current
    task and all remaining tasks are skipped.

    Use this for recoverable situations like API quota exhaustion where
    retrying later will succeed.

    Exit code: 2 (distinct from 0=success and 1=error).
    """

    exit_code = 2


class FatalError(IngestifyError):
    """Raised by a source or loader to signal that the run cannot continue and
    must fail loudly. Unlike StopProcessing, this is NOT recoverable by
    retrying later — e.g. a deactivated account or an otherwise misconfigured
    source.

    Already-processed datasets are preserved, but the run aborts with a
    non-zero exit so schedulers surface it as a failure instead of a silent
    success. It propagates out of find_datasets/collect rather than being
    swallowed as a skipped find_datasets.

    Exit code: 1.
    """

    exit_code = 1
