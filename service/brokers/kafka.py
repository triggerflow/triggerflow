from enum import Enum
from typing import Optional, List

from .kafka_client import KafkaClient
from .broker import Broker


class KafkaSASLAuthMode(Enum):
    SASL_PLAINTEXT = 0


class KafkaBroker(Broker):
    def __init__(self, broker_list: List[str], topic: str, auth_mode: KafkaSASLAuthMode, username: str, password: str):
        super().__init__()
        self.topic = topic
        self.__kafka_client = KafkaClient(brokers=broker_list, username=username, password=password)
