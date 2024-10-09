from abc import ABC, abstractmethod

from .task_summary import TaskSummary


class Task(ABC):
    @abstractmethod
    def run(self) -> TaskSummary:
        pass
