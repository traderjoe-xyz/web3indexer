from datetime import datetime
import os

from kafka import KafkaProducer
import structlog
from web3 import Web3
from web3.middleware import geth_poa_middleware

from .constants import PROCESS_LOG_KAFKA_TOPIC
from .task import FetchBlockTask, ProcessLogTask


logger = structlog.get_logger()


class BlockFetcher:
    """
    Handles fetching information for a given block and scheduling `ProcessLog` tasks.
    """

    MAX_RETRIES = 5

    def __init__(self, w3: Web3, producer: KafkaProducer):
        self.w3 = w3
        self.producer = producer

    def fetch_with_retry(self, block_number: int):
        try:
            self.fetch(block_number)
        except Exception as e:
            # TODO: Handle retries
            logger.error(e)

    def fetch(self, block_number: int):
        logger.info("Fetching block", block_number=block_number)

        block = self.w3.eth.get_block(block_number)

        for transaction in block.transactions:
            txn_receipt = self.w3.eth.get_transaction_receipt(transaction)

            for log_index, log in enumerate(txn_receipt.logs):
                self.producer.send(
                    PROCESS_LOG_KAFKA_TOPIC,
                    value={
                        "block_number": block_number,
                        "log": Web3.toJSON(log),
                        "log_index": log_index,
                        "timestamp": block.timestamp,
                    },
                )
