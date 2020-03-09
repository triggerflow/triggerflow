import json

from enum import Enum
from typing import Optional, List
from uuid import uuid1
from confluent_kafka import Producer

from ..model import EventSource


class KafkaAuthMode(Enum):
    NONE = 0
    SASL_PLAINTEXT = 1


class KafkaEventSource(EventSource):

    def __init__(self,
                 broker_list:  Optional[List[str]] = None,
                 topic: Optional[str] = None,
                 auth_mode: Optional[KafkaAuthMode] = KafkaAuthMode.NONE,
                 username: Optional[str] = None,
                 password: Optional[str] = None):

        self.broker_list = broker_list
        self.topic = topic
        self.auth_mode = auth_mode
        if auth_mode == KafkaAuthMode.SASL_PLAINTEXT:
            self.username = username
            self.password = password

    def _set_name(self, prefix):
        self.name = '{}-kafka-eventsource'.format(prefix)

    def publish_cloudevent(self, cloudevent):
        config = {'bootstrap.servers': ','.join(self.broker_list),
                  'group.id': str(uuid1())}

        if self.auth_mode == 'SASL_PLAINTEXT':
            # append Event streams specific config
            config.update({'ssl.ca.location': '/etc/ssl/certs/',
                           'sasl.mechanisms': 'PLAIN',
                           'sasl.username': self.username,
                           'sasl.password': self.password,
                           'security.protocol': 'sasl_ssl'
                           })

        def delivery_callback(err, msg):
            if err:
                print('Failed delivery: {}'.format(err))
            else:
                print('Message delivered: {} {} {}'.format(msg.topic(), msg.partition(), msg.offset()))

        kafka_producer = Producer(**config)
        kafka_producer.produce(topic=self.topic,
                               value=json.dumps(cloudevent), callback=delivery_callback)
        kafka_producer.flush()

    @property
    def json(self):
        d = vars(self)
        d['auth_mode'] = self.auth_mode.name
        d['class'] = self.__class__.__name__
        return d
