from dataclasses import dataclass, field
from datetime import datetime

from ingestify.utils import utcnow


class EventBus:
    def __init__(self):
        self.dispatchers = []

    def register(self, dispatcher):
        self.dispatchers.append(dispatcher)

    def dispatch(self, event):
        for dispatcher in self.dispatchers:
            dispatcher.dispatch(event)


class EventRepository:
    def __init__(self):
        self.events = []

    def save(self, event):
        self.events.append(event)


class EventWriter:
    def __init__(self, event_repository: EventRepository):
        self.event_repository = event_repository

    def dispatch(self, event):
        self.event_repository.save(event)


@dataclass
class DomainEvent:
    def __post_init__(self):
        self.occurred_at = utcnow()
