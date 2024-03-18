import logging

from .dispatcher import Dispatcher
from .domain_event import DomainEvent
from .subscriber import Subscriber


logger = logging.getLogger(__name__)


class Publisher(Dispatcher):
    def __init__(self):
        self.subscribers: list[Subscriber] = []

    def dispatch(self, event: DomainEvent):
        for subscriber in self.subscribers:
            try:
                subscriber.handle(event)
            except Exception:
                logger.exception(f"Failed to handle {event} by {subscriber}")

    def add_subscriber(self, subscriber: Subscriber):
        self.subscribers.append(subscriber)
