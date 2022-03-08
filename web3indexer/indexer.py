import queue
import os
import time
from threading import Thread

from web3 import Web3

from .dispatcher import Dispatcher
from .task import Task
from .collector import (
    UniSwapFactoryCollector,
    UniSwapPairDataCollector,
    UniSwapPairSwapCollector
)
from .worker import Worker, STOP_TASK


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
    #dispatcher.put(
    #    Task(
    #        "UniSwapFactoryCollector",
    #        "0x9Ad6C38BE94206cA50bb0d90783181662f0Cfa10",
    #        None,
    #    )
    #)
    # Example of just listening to 4 pairs.
    pairs = [
        '0xa389f9430876455c36478deea9769b7ca4e3ddb1',
        '0xed8cbd9f0ce3c6986b22002f03c6475ceb7a6256',
        '0xf4003f4efbe8691b60249e6afbd307abe7758adb',
        '0xfe15c2695f1f920da45c30aae47d11de51007af9',
    ]
    for pair in pairs:
        dispatcher.put(
            Task(
                "UniSwapPairDataCollector",
                Web3.toChecksumAddress(pair),
                None,
            )
        )
    main_thread = Thread(target=worker.run)
    try:
        main_thread.start()
        main_thread.join()
    except KeyboardInterrupt:
        dispatcher.put(STOP_TASK)


def main():
    run()

