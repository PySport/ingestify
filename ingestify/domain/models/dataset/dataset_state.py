from enum import Enum


class DatasetState(str, Enum):
    SCHEDULED = "SCHEDULED"
    PARTIAL = "PARTIAL"
    COMPLETE = "COMPLETE"

    @property
    def is_complete(self):
        return self == DatasetState.COMPLETE
