from .task import Task


class TaskSet:
    def __init__(self, tasks=None):
        self.tasks = tasks or []

    def add(self, task: Task):
        self.tasks.append(task)

    def __len__(self):
        return len(self.tasks)

    def __iter__(self):
        return iter(self.tasks)

    def __add__(self, other: "TaskSet"):
        return TaskSet(self.tasks + other.tasks)

    def __bool__(self):
        return len(self) > 0
