from enum import Enum
from typing import Optional, List
from .model import CloudEventSource


class KafkaSASLAuthMode(Enum):
    SASL_PLAINTEXT = 0


class KafkaCloudEventSource(CloudEventSource):
    name = 'Kafka'

    def __init__(self, broker_list: List[str], topic: str, auth_mode: Optional[KafkaSASLAuthMode] = None,
                 username: Optional[str] = None, password: Optional[str] = None):
        super().__init__()
        self.broker_list = broker_list
        self.topic = topic
        self.auth_mode = auth_mode
        self.username = username
        self.password = password

    @property
    def dict(self):
        d = vars(self)
        if self.auth_mode is not None:
            d['auth_mode'] = self.auth_mode.name
        print(d)
        return {self.__class__.__name__: d}
