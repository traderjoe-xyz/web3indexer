import json
import os
from time import sleep
from threading import Thread
from typing import List

from kafka import KafkaConsumer, KafkaProducer
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
    consumer = KafkaConsumer(
        "test-topic",
        api_version=(2, 0, 2),
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="my-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    for msg in consumer:
        print("Consuming:", msg)


def main():
    run()
