import json
import os
from time import sleep
from threading import Thread
from typing import List

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from pymongo import MongoClient
import structlog

from .collector import GenericEventCollector, _read_file  # XXX
from .crud import (
    get_all_contracts,
    get_last_scanned_event,
)
from .dispatcher import Dispatcher
from .task import FetchBlockTask, ScrapeTask
from .worker import Worker, STOP_TASK


log = structlog.get_logger()


def run():
    # admin_client = KafkaAdminClient(
    #     api_version=(2, 0, 2), bootstrap_servers="localhost:9092"
    # )
    # topic_list = [
    #     NewTopic(name="test-topic", num_partitions=1, replication_factor=1)
    # ]
    # admin_client.create_topics(new_topics=topic_list)

    producer = KafkaProducer(
        api_version=(2, 0, 2),
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )
    for i in range(10):
        print("Sending:", i)
        producer.send("test-topic", value={"number": i})
        sleep(1)


def main():
    run()
