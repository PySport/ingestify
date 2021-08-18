from abc import ABC, abstractmethod

from ingestify.utils import ComponentFactory, ComponentRegistry

from .dataset import Dataset

sink_registry = ComponentRegistry()


class Sink(ABC, metaclass=sink_registry.metaclass):
    @abstractmethod
    def upsert(self, dataset: Dataset, data, params: dict):
        pass


sink_factory = ComponentFactory.build_factory(Sink, sink_registry)
