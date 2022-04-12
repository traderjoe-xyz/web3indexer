import json

from kafka import KafkaProducer
import structlog
from web3 import Web3

from .constants import PROCESS_LOG_KAFKA_TOPIC


logger = structlog.get_logger()


class BlockFetcher:
    """
    Handles fetching information for a given block and scheduling `ProcessLog` tasks.
    """

    MAX_RETRIES = 5

    def __init__(self, w3: Web3):
        self.w3 = w3
        self.producer = KafkaProducer(
            api_version=(2, 0, 2),
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )

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
