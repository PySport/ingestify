from .dispatcher import Dispatcher


class EventBus:
    def __init__(self):
        self.dispatchers: list[Dispatcher] = []

    def register(self, dispatcher: Dispatcher):
        self.dispatchers.append(dispatcher)

    def dispatch(self, event):
        for dispatcher in self.dispatchers:
            dispatcher.dispatch(event)
