from abc import ABC, abstractmethod
from datetime import datetime
from pydantic import BaseModel, Field

from ingestify.utils import utcnow


class DomainEvent(BaseModel, ABC):
    occurred_at: datetime = Field(default_factory=utcnow)

    @property
    @abstractmethod
    def event_type(self) -> str:
        pass
