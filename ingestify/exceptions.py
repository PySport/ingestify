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
