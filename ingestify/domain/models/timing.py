from datetime import datetime
from typing import Optional, Any
from pydantic import BaseModel, ConfigDict


class Timing(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    started_at: datetime
    ended_at: datetime
    metadata: Optional[dict[str, Any]] = None

    @property
    def duration(self):
        return self.ended_at - self.started_at
