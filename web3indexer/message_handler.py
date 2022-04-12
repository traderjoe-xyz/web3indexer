from datetime import datetime
import json
import os

import structlog
from web3 import Web3
from web3.datastructures import AttributeDict
from web3.middleware import geth_poa_middleware

from .block_fetcher import BlockFetcher
from .constants import FETCH_BLOCK_KAFKA_TOPIC, PROCESS_LOG_KAFKA_TOPIC, SUPPORTED_KAFKA_TOPICS
from .log_processor import LogProcessor


log = structlog.get_logger()


class MessageHandler:

    def __init__(self):
        endpoint_uri = os.environ["ENDPOINT_URL"]
        self.w3 = Web3(Web3.HTTPProvider(endpoint_uri))
        # Required for Avalanche, see https://web3py.readthedocs.io/en/stable/middleware.html#geth-style-proof-of-authority
        self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    def handle_message(self, topic_partition, consumer_record):
        if topic_partition.topic == FETCH_BLOCK_KAFKA_TOPIC:
            block_fetcher = BlockFetcher(self.w3)
            block_fetcher.fetch_with_retry(consumer_record.value)
        elif topic_partition.topic == PROCESS_LOG_KAFKA_TOPIC:
            log_processor = LogProcessor(self.w3)
            log_processor.process_with_retry(
                consumer_record.value["block_number"],
                AttributeDict(json.loads(consumer_record.value["log"])),
                consumer_record.value['log_index'],
                datetime.fromtimestamp(consumer_record.value['timestamp']),
            )
        else:
            log.error("Unsupported Kafka topic: {}".format(topic_partition.topic))
