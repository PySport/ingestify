from typing import Protocol

from .domain_event import DomainEvent


class Dispatcher(Protocol):
    def dispatch(self, event: DomainEvent):
        pass
