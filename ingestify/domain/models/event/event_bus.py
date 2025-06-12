import logging


from .dispatcher import Dispatcher


logger = logging.getLogger(__name__)


class QueueForwarder:
    def __init__(self, queue):
        self.queue = queue

    def dispatch(self, event):
        self.queue.put(event)


class EventBus:
    def __init__(self):
        self.dispatchers: list[Dispatcher] = []

    def register(self, dispatcher: Dispatcher):
        self.dispatchers.append(dispatcher)

        def unregister():
            self.dispatchers.remove(dispatcher)

        return unregister

    def register_queue(self, queue):
        return self.register(QueueForwarder(queue))

    def dispatch(self, event):
        for dispatcher in self.dispatchers:
            try:
                dispatcher.dispatch(event)
            except Exception as e:
                logger.exception(f"Failed to handle {event}")
                raise Exception(f"Failed to handle {event}") from e
