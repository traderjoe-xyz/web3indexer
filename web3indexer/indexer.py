import json
import os
import time
from threading import Thread

from pymongo import MongoClient
import structlog
from web3 import Web3

from .collector import GenericEventCollector, _read_file  # XXX
from .crud import get_all_contracts, get_last_scanned_event, insert_if_not_exists
from .dispatcher import Dispatcher
from .task import Task, ScrapeTask, ProcessBlockTask
from .worker import Worker, STOP_TASK


log = structlog.get_logger()


def add_nft_contracts(db, dispatcher):
    """
    Helper function to start extractions for
    NFT contracts.
    """
    for contract in get_all_contracts(db):
        events = [
            event
            for event in contract['abi']
            if event['type'] == 'event'
        ]
        last_block = get_last_scanned_event(db, contract['address'])
        for event in events:
            dispatcher.put(
                ScrapeTask(
                    "GenericEventCollector",
                    contract['abi'],
                    contract['address'],
                    event['name'],
                    last_block,
                    0,
                )
            )


def fetch_block(dispatcher, block_number):
    dispatcher.put(
        ProcessBlockTask(
            block_number=block_number
        )
    )



def run():
    dispatcher = Dispatcher()
    endpoint_uri = os.environ['ENDPOINT_URL']
    connection = MongoClient(os.environ['MONGODB_URI'])
    db = connection.web3indexer
    worker = Worker(endpoint_uri, dispatcher, max_collectors=100)

    worker.add_collector_by_name(
        'GenericEventCollector',
        GenericEventCollector(db),
    )

    abi = json.loads(_read_file('abi/ERC721.json'))
    fetch_block(dispatcher, 13087687)
    # addresses = [line for line in _read_file('addresses').split('\n') if line]
    # for address in addresses:
    #     insert_if_not_exists(db, address, abi)

    # add_nft_contracts(db, dispatcher)

    main_thread = Thread(target=worker.run)
    try:
        main_thread.start()
        main_thread.join()
    except KeyboardInterrupt:
        dispatcher.put(STOP_TASK)
    finally:
        connection.close()


def main():
    run()

