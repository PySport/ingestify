from abc import abstractmethod, ABC


class Task(ABC):
    @abstractmethod
    def run(self):
        pass


