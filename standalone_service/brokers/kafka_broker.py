import json
import logging
from enum import Enum
from typing import List

from confluent_kafka import TopicPartition, Consumer

from api.utils import load_config_yaml
from standalone_service.libs.kafka_client import KafkaClient
from .broker import Broker


class KafkaSASLAuthMode(Enum):
    SASL_PLAINTEXT = 0


class KafkaBroker(Broker):
    def __init__(self, broker_list: List[str], topic: str, auth_mode: KafkaSASLAuthMode, username: str, password: str):
        super().__init__()
        self.topic = topic
        self.__kafka_client = KafkaClient(brokers=broker_list, username=username, password=password)
        self.__consumer = None
        if not self.__kafka_client.topic_exists(topic):
            self.__kafka_client.create_topic(topic)
        self.__consumer = self.__kafka_client.create_consumer(topic)

    def poll(self):
        return self.__consumer.poll(timeout=1.0)

    def body(self, record):
        return json.loads(record.value())

    def commit(self, record):
        pass

    def __get_offset_list(events):
        offsets = []
        for message in events:
            # Add one to the offset, otherwise we'll consume this message again.
            # That's just how Kafka works, you place the bookmark at the *next* message.
            offsets.append(TopicPartition(message.topic(), message.partition(), message.offset() + 1))

        return offsets
