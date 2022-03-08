from concurrent.futures import ThreadPoolExecutor
import time

from web3 import Web3

from .task import Task


# Special task for stopping the worker.
STOP_TASK = Task("stop", None, None)


class Worker:
    """
    Manages collectors and the w3 connection.
    """

    def __init__(self, endpoint_uri, dispatcher, max_collectors=None):
        self.dispatcher = dispatcher
        self.max_collectors = max_collectors
        self.collectors = {}
        self.w3 = Web3(Web3.HTTPProvider(endpoint_uri))

    def run(self):
        with ThreadPoolExecutor(max_workers=self.max_collectors) as executor:
            while True:
                if int(time.time()) % 60 == 0:
                    print('Queue size:', self.dispatcher.size)
                task = self.dispatcher.get()
                # Special case the stop task.
                if task is STOP_TASK:
                    return
                executor.submit(
                    self.collectors[task.collector].collect,
                    self.dispatcher,
                    self.w3,
                    task,
                )

    def run_single(self):
        # Used for debugging.
        while True:
            task = self.dispatcher.get()
            # Special case the stop task.
            if task is STOP_TASK:
                return
            self.collectors[task.collector].collect(
                self.dispatcher,
                self.w3,
                task,
            )

    def add_collector_by_name(self, name, cls):
        self.collectors[name] = cls()

