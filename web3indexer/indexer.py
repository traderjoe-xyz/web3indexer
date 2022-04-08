import json
import os
from threading import Thread
from typing import List

from pymongo import MongoClient
import structlog

from .collector import GenericEventCollector, _read_file  # XXX
from .crud import (
    get_all_contracts,
    get_last_scanned_event,
)
from .dispatcher import Dispatcher
from .task import ProcessBlockTask, ScrapeTask
from .worker import Worker, STOP_TASK


log = structlog.get_logger()

TEST_ERC721_BLOCK_NUMBER = 13087687
TEST_ERC1155_TRANSFER_SINGLE_BLOCK_NUMBER = 13134106
TEST_ERC1155_TRANSFER_BATCH_BLOCK_NUMBER = 8316310


def add_nft_contracts(db, dispatcher):
    """
    Helper function to start extractions for
    NFT contracts.
    """
    for contract in get_all_contracts(db):
        events = [
            event for event in contract["abi"] if event["type"] == "event"
        ]
        last_block = get_last_scanned_event(db, contract["address"])
        for event in events:
            dispatcher.put(
                ScrapeTask(
                    "GenericEventCollector",
                    contract["abi"],
                    contract["address"],
                    event["name"],
                    last_block,
                    0,
                )
            )


def fetch_blocks(dispatcher, block_numbers: List[int]):
    for block_number in block_numbers:
        dispatcher.put(ProcessBlockTask(block_number=block_number))


def run():
    dispatcher = Dispatcher()
    endpoint_uri = os.environ["ENDPOINT_URL"]
    connection = MongoClient(os.environ["MONGODB_URI"])
    db = connection.web3indexer
    worker = Worker(endpoint_uri, dispatcher, db, max_collectors=100)

    fetch_blocks(
        dispatcher,
        [
            TEST_ERC721_BLOCK_NUMBER,
            TEST_ERC1155_TRANSFER_SINGLE_BLOCK_NUMBER,
            TEST_ERC1155_TRANSFER_BATCH_BLOCK_NUMBER,
        ],
    )

    # worker.add_collector_by_name(
    #     "GenericEventCollector",
    #     GenericEventCollector(db),
    # )

    # abi = json.loads(_read_file("abi/ERC721.json"))
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
