import json
from enum import Enum
from typing import List

from service.libs.kafka_client import KafkaClient
from .broker import Broker


class KafkaSASLAuthMode(Enum):
    SASL_PLAINTEXT = 0


class KafkaBroker(Broker):

    def __init__(self, broker_list: List[str], topic: str, auth_mode: KafkaSASLAuthMode, username: str, password: str):
        super().__init__()
        self.topic = topic
        self.__kafka_client = KafkaClient(brokers=broker_list, username=username, password=password)
        if not self.__kafka_client.topic_exists(topic):
            self.__kafka_client.create_topic(topic)

        self.__consumer = self.__kafka_client.create_consumer(topic)

    def poll(self):
        return self.__consumer.poll()

    def body(self, record):
        return json.loads(record.value())

    def commit(self, record):
        pass
