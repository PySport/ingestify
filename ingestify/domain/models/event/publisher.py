from .dispatcher import Dispatcher
from .domain_event import DomainEvent
from .subscriber import Subscriber


class Publisher(Dispatcher):
    def __init__(self):
        self.subscribers: list[Subscriber] = []

    def dispatch(self, event: DomainEvent):
        for subscriber in self.subscribers:
            subscriber.handle(event)

    def add_subscriber(self, subscriber: Subscriber):
        self.subscribers.append(subscriber)
