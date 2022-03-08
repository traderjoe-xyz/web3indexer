import queue
import os
import time
from threading import Thread

import structlog
from web3 import Web3

from .dispatcher import Dispatcher
from .task import Task
from .collector import (
    UniSwapFactoryCollector,
    UniSwapPairDataCollector,
    UniSwapPairSwapCollector
)
from .worker import Worker, STOP_TASK


log = structlog.get_logger()


def run():
    dispatcher = Dispatcher()
    endpoint_uri = os.environ['ENDPOINT_URL']
    worker = Worker(endpoint_uri, dispatcher, max_collectors=100)

    collectors =  [
        UniSwapFactoryCollector,
        UniSwapPairDataCollector,
        UniSwapPairSwapCollector,
    ]
    for cls in collectors:
        worker.add_collector_by_name(cls.__name__, cls)
    dispatcher.put(
        Task(
            "UniSwapFactoryCollector",
            "0x9Ad6C38BE94206cA50bb0d90783181662f0Cfa10",
            None,
        )
    )
    main_thread = Thread(target=worker.run)
    try:
        main_thread.start()
        log.info("Stared main thread")
        main_thread.join()
    except KeyboardInterrupt:
        log.info("Shutting down indexer")
        # Pop any uncompleted tasks.
        while dispatcher.size:
            dispatcher.get()
        dispatcher.put(STOP_TASK)


def main():
    run()

