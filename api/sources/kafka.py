from enum import Enum
from typing import Optional, List
from .model import CloudEventSource


class KafkaAuthMode(Enum):
    NONE = 0
    SASL_PLAINTEXT = 1


class KafkaCloudEventSource(CloudEventSource):
    def __init__(self, broker_list: List[str], topic: str, auth_mode: Optional[KafkaAuthMode] = KafkaAuthMode.NONE,
                 username: Optional[str] = None, password: Optional[str] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.broker_list = broker_list
        self.topic = topic
        self.auth_mode = auth_mode
        self.username = username
        self.password = password

    @property
    def json(self):
        d = super().json
        d['spec'] = vars(self)
        d['spec']['auth_mode'] = self.auth_mode.name
        d['class'] = self.__class__.__name__
        return d
