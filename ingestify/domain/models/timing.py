from dataclasses import dataclass
from datetime import datetime


@dataclass
class Timing:
    name: str
    start: datetime
    end: datetime
    metadata: dict | None = None

    @property
    def duration(self):
        return self.end - self.start
