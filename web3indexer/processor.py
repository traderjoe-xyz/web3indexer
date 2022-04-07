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
        print("BLOCK PROCESSOR RECEIVED TASK:", task)