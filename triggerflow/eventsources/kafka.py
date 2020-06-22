import json

from typing import Optional, List
from uuid import uuid1
from confluent_kafka import Producer

from triggerflow.eventsources.model import EventSource


class KafkaEventSource(EventSource):

    def __init__(self,
                 broker_list:  List[str],
                 topic: Optional[str] = None,
                 auth_mode: Optional[str] = None,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 *args, **kwargs):

        super().__init__(*args, **kwargs)

        self.broker_list = broker_list
        self.topic = topic
        self.auth_mode = auth_mode if auth_mode is not None else 'NONE'

        if auth_mode == 'SASL_PLAINTEXT':
            self.username = username
            self.password = password

    def set_stream(self, stream_id: str):
        self.topic = stream_id

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
                               value=cloudevent.MarshalJSON(json.dumps).read(),
                               callback=delivery_callback)
        kafka_producer.flush()

    def get_json_eventsource(self):
        parameters = vars(self).copy()
        del parameters['name']
        return {'name': self.name, 'class': self.__class__.__name__, 'parameters': parameters}
