import logging


from .dispatcher import Dispatcher


logger = logging.getLogger(__name__)


class EventBus:
    def __init__(self):
        self.dispatchers: list[Dispatcher] = []

    def register(self, dispatcher: Dispatcher):
        self.dispatchers.append(dispatcher)

    def dispatch(self, event):

        for dispatcher in self.dispatchers:
            try:
                dispatcher.dispatch(event)
            except Exception as e:
                logger.exception(f"Failed to handle {event}")
                raise Exception(f"Failed to handle {event}") from e
