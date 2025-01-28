class IngestifyError(Exception):
    pass


class ConfigurationError(IngestifyError):
    pass


class DuplicateFile(IngestifyError):
    pass


class SaveError(IngestifyError):
    pass
