import logging
from enum import Enum
from typing import Union, List, Optional, NewType


logger = logging.getLogger(__name__)


class DatasetState(str, Enum):
    SCHEDULED = "SCHEDULED"
    PARTIAL = "PARTIAL"
    COMPLETE = "COMPLETE"

    @property
    def is_complete(self):
        return self == DatasetState.COMPLETE

