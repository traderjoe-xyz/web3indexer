import json

import structlog
from web3 import Web3

from .crud import insert_event
from .task import Task, ScrapeTask


log = structlog.get_logger()


class BlockProcessor:
    """
    Custom class for fetching, parsing and processing a block from the blockchain.
    """

    def process(self, dispatcher, w3, task):
        print("PROCESSING TASK:", task, task.block_number)
        block = w3.eth.get_block(task.block_number)
        print("GOT BLOCK:", block)