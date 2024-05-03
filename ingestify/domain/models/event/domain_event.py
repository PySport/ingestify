from abc import abstractmethod, ABC
from dataclasses import dataclass


@dataclass
class DomainEvent(ABC):
    @property
    @abstractmethod
    def event_type(self) -> str:
        pass
