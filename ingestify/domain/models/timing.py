from datetime import datetime
from typing import Optional, Any
from pydantic import BaseModel, ConfigDict


class Timing(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    start: datetime
    end: datetime
    metadata: Optional[dict[str, Optional[Exception | Any]]] = None

    @property
    def duration(self):
        return self.end - self.start
