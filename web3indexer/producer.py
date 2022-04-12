import json

from kafka import  KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import structlog

from .constants import FETCH_BLOCK_KAFKA_TOPIC, SUPPORTED_KAFKA_TOPICS


log = structlog.get_logger()

TEST_ERC721_BLOCK_NUMBER = 13087687
TEST_ERC1155_TRANSFER_SINGLE_BLOCK_NUMBER = 13134106
TEST_ERC1155_TRANSFER_BATCH_BLOCK_NUMBER = 8316310


def run():
    # Create topics if not already created
    admin_client = KafkaAdminClient(
        api_version=(2, 0, 2), bootstrap_servers="localhost:9092"
    )
    topics = admin_client.list_topics()
    for supported_topic in SUPPORTED_KAFKA_TOPICS:
        if supported_topic not in topics:
            topic_list = [
                # TODO: Configure num_partitions + replication_factor
                NewTopic(name=supported_topic, num_partitions=1, replication_factor=1)
            ]
            admin_client.create_topics(new_topics=topic_list)

    producer = KafkaProducer(
        api_version=(2, 0, 2),
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    # TODO: Find blocks that we need to fetch
    for block_number in [
        TEST_ERC721_BLOCK_NUMBER,
        TEST_ERC1155_TRANSFER_SINGLE_BLOCK_NUMBER,
        TEST_ERC1155_TRANSFER_BATCH_BLOCK_NUMBER,
    ]:
        producer.send(FETCH_BLOCK_KAFKA_TOPIC, value=block_number)


def main():
    run()
