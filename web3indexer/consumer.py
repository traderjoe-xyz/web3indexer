import json
from datetime import datetime
import os
import sys
from time import sleep
from queue import Queue
from threading import get_ident, Thread
from multiprocessing import Process
from typing import List

from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from pymongo.database import Database
import structlog
from web3 import Web3
from web3.datastructures import AttributeDict
from web3.middleware import geth_poa_middleware

from .block_fetcher import BlockFetcher
from .constants import FETCH_BLOCK_KAFKA_TOPIC, PROCESS_LOG_KAFKA_TOPIC, SUPPORTED_KAFKA_TOPICS
from .collector import GenericEventCollector, _read_file  # XXX
from .crud import (
    get_all_contracts,
    get_last_scanned_event,
)
from .dispatcher import Dispatcher
from .log_processor import LogProcessor
from .task import FetchBlockTask, ScrapeTask
from .worker import Worker, STOP_TASK


log = structlog.get_logger()


def _process_msg(msg_queue: Queue):
    msg = msg_queue.get(
        timeout=60
    )  # Set timeout to care for POSIX<3.0 and Windows.

    endpoint_uri = os.environ["ENDPOINT_URL"]
    w3 = Web3(Web3.HTTPProvider(endpoint_uri))
    # Required for Avalanche, see https://web3py.readthedocs.io/en/stable/middleware.html#geth-style-proof-of-authority
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    endpoint_uri = os.environ["ENDPOINT_URL"]
    connection = MongoClient(os.environ["MONGODB_URI"])
    db = connection.web3indexer

    producer = KafkaProducer(
        api_version=(2, 0, 2),
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    for topic_partition, consumer_records in msg.items():
        for consumer_record in consumer_records:
            log.info(
                "#{}T{} - Received message: {} from topic: {} and partition: {}".format(
                    os.getpid(),
                    get_ident(),
                    consumer_record.value,
                    topic_partition.topic,
                    topic_partition.partition,
                )
            )
            if topic_partition.topic == FETCH_BLOCK_KAFKA_TOPIC:
                block_fetcher = BlockFetcher(w3, producer)
                block_fetcher.fetch_with_retry(consumer_record.value)
            elif topic_partition.topic == PROCESS_LOG_KAFKA_TOPIC:
                log_processor = LogProcessor(w3, db)
                log_processor.process_with_retry(
                    consumer_record.value["block_number"],
                    AttributeDict(json.loads(consumer_record.value["log"])),
                    consumer_record.value['log_index'],
                    datetime.fromtimestamp(consumer_record.value['timestamp']),
                )
            else:
                log.error("Unsupported Kafka topic: {}".format(topic_partition.topic))

    msg_queue.task_done()


def _consume(topic: str, group_id: str):
    log.info(
        "#{} - Starting consumer group={}, topic={}".format(
            os.getpid(), group_id, topic
        )
    )
    consumer = KafkaConsumer(
        topic,
        api_version=(2, 0, 2),
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    msg_queue = Queue(maxsize=100)

    while True:
        try:
            msg = consumer.poll(1)
            if not msg:
                continue
            msg_queue.put(msg)
            t = Thread(target=_process_msg, args=(msg_queue,))
            t.start()
        except Exception:
            log.exception("#%s - Worker terminated.", os.getpid())
            consumer.close()


def run(topic: str, group_id: str):
    """
    Simple program that consumes messages from Kafka topic and prints to
    STDOUT.
    """
    workers = []
    max_num_workers = 4

    while True:
        num_alive = len([w for w in workers if w.is_alive()])
        if max_num_workers == num_alive:
            continue
        for _ in range(max_num_workers - num_alive):
            p = Process(
                target=_consume,
                daemon=True,
                args=(
                    topic,
                    group_id,
                ),
            )
            p.start()
            workers.append(p)
            log.info("Starting worker #{}".format(p.pid))


def main():
    args = sys.argv
    if len(args) != 3:
        log.error("Expected `poetry run consumer <topic> <group_id>`")
        return

    topic = args[1]
    if not topic in SUPPORTED_TOPICS:
        log.error(
            "Unsupported topic, expected one of: {}".format(SUPPORTED_TOPICS)
        )
        return

    group_id = args[2]

    log.info(
        "Spinning up consumers for topic: {}, group_id: {}".format(
            topic, group_id
        )
    )

    run(topic, group_id)
