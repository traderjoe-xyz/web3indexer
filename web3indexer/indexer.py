import os
import json
import time
from threading import Thread

import structlog
from web3 import Web3

from .dispatcher import Dispatcher
from .task import Task, ScrapeTask
from .collector import GenericEventCollector, _read_file  # XXX
from .worker import Worker, STOP_TASK


log = structlog.get_logger()


def run():
    dispatcher = Dispatcher()
    endpoint_uri = os.environ['ENDPOINT_URL']
    worker = Worker(endpoint_uri, dispatcher, max_collectors=100)

    worker.add_collector_by_name('GenericEventCollector', GenericEventCollector())

    party_animals_address = "0x880Fe52C6bc4FFFfb92D6C03858C97807a900691"
    party_animals_abi = json.loads(_read_file('abi/PartyAnimals.json'))
    party_animals_events = [
        event
        for event in party_animals_abi
        if event['type'] == 'event'
    ]
    from_block = 11000000


    for event in party_animals_events:
        dispatcher.put(
            ScrapeTask(
                "GenericEventCollector",
                party_animals_abi,
                party_animals_address,
                event['name'],
                from_block,
            )
        )

    # dispatcher.put(
    #     Task(
    #         "UniSwapFactoryCollector",
    #         "0x9Ad6C38BE94206cA50bb0d90783181662f0Cfa10",
    #         None,
    #     )
    # )

    main_thread = Thread(target=worker.run)
    try:
        main_thread.start()
        log.info("Started main thread")
        main_thread.join()
    except KeyboardInterrupt:
        log.info("Shutting down indexer")
        # Pop any uncompleted tasks.
        while dispatcher.size:
            dispatcher.get()
        dispatcher.put(STOP_TASK)


def main():
    run()

