from dataclasses import dataclass

from ingestify.utils import utcnow


@dataclass
class DomainEvent:
    def __post_init__(self):
        self.occurred_at = utcnow()
