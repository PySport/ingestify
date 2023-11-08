from abc import abstractmethod, ABC
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol

from ingestify.utils import utcnow


class Dispatcher(Protocol):
    def dispatch(self, event: "DomainEvent"):
        pass


class Subscriber(ABC):
    def __init__(self, store):
        self.store = store

    @abstractmethod
    def handle(self, event: "DomainEvent"):
        pass


class EventPublisher(Dispatcher):
    def __init__(self):
        self.subscribers: list[Subscriber] = []

    def dispatch(self, event: "DomainEvent"):
        for subscriber in self.subscribers:
            subscriber.handle(event)

    def add_subscriber(self, subscriber: Subscriber):
        self.subscribers.append(subscriber)


class EventBus:
    def __init__(self):
        self.dispatchers: list[Dispatcher] = []

    def register(self, dispatcher: Dispatcher):
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
