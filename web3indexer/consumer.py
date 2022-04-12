import json
from multiprocessing import Process
import os
from queue import Queue
import sys
from threading import get_ident, Thread

from kafka import KafkaConsumer
import structlog

from .constants import SUPPORTED_KAFKA_TOPICS
from .message_handler import MessageHandler


log = structlog.get_logger()


def _process_msg(msg_queue: Queue):
    msg = msg_queue.get(
        timeout=60
    )

    msg_handler = MessageHandler()

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
            msg_handler.handle_message(topic_partition, consumer_record)


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
            msg = consumer.poll()
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
    if not topic in SUPPORTED_KAFKA_TOPICS:
        log.error(
            "Unsupported topic, expected one of: {}".format(SUPPORTED_KAFKA_TOPICS)
        )
        return

    group_id = args[2]

    log.info(
        "Spinning up consumers for topic: {}, group_id: {}".format(
            topic, group_id
        )
    )

    run(topic, group_id)
