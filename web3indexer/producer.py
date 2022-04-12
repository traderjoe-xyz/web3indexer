import json
import os
from time import sleep
from threading import Thread
from typing import List

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from pymongo import MongoClient
import structlog

from .constants import FETCH_BLOCK_KAFKA_TOPIC, SUPPORTED_KAFKA_TOPICS
from .collector import GenericEventCollector, _read_file  # XXX
from .crud import (
    get_all_contracts,
    get_last_scanned_event,
)
from .dispatcher import Dispatcher
from .task import FetchBlockTask, ScrapeTask
from .worker import Worker, STOP_TASK


log = structlog.get_logger()

TEST_ERC721_BLOCK_NUMBER = 13087687
TEST_ERC1155_TRANSFER_SINGLE_BLOCK_NUMBER = 13134106
TEST_ERC1155_TRANSFER_BATCH_BLOCK_NUMBER = 8316310


def run():
    admin_client = KafkaAdminClient(
        api_version=(2, 0, 2), bootstrap_servers="localhost:9092"
    )
    topics = admin_client.list_topics()
    for supported_topic in topics:
        if supported_topic not in topics:
            topic_list = [
                NewTopic(name=supported_topic, num_partitions=1, replication_factor=1)
            ]
            admin_client.create_topics(new_topics=topic_list)

    producer = KafkaProducer(
        api_version=(2, 0, 2),
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    for block_number in [
        TEST_ERC721_BLOCK_NUMBER,
        TEST_ERC1155_TRANSFER_SINGLE_BLOCK_NUMBER,
        TEST_ERC1155_TRANSFER_BATCH_BLOCK_NUMBER,
    ]:
        producer.send(FETCH_BLOCK_KAFKA_TOPIC, value=block_number)


def main():
    run()
