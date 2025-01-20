from abc import ABC, abstractmethod

from .dataset import Dataset


class Sink(ABC):
    @abstractmethod
    def upsert(self, dataset: Dataset, data, params: dict):
        pass
