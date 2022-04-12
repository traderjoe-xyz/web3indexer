from queue import Queue
import time

from .task import (
    ScheduledTask,
    ScrapeTask,
    Task,
)


class Dispatcher:
    def __init__(self):
        self.queue = Queue()

    def put(self, task):
        self.queue.put(task)

    def get(self):
        while True:
            task = self.queue.get()
            if isinstance(task, (Task, ScrapeTask)):
                break
            if isinstance(task, ScheduledTask):
                if time.time() >= task.not_before:
                    task = task.task
                    break
                self.put(task)
            time.sleep(0.1)

        return task

    def schedule(self, task, n):
        self.put(ScheduledTask(task, time.time() + n))

    def join(self):
        self.queue.join()

    @property
    def size(self):
        return self.queue.qsize()
