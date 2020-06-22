import pika
import json
from typing import Optional

from triggerflow.eventsources.model import EventSource


class RabbitMQEventSource(EventSource):
    def __init__(self,
                 amqp_url: str,
                 queue: Optional[str] = None,
                 *args, **kwargs):

        super().__init__(*args, **kwargs)

        self.queue = queue
        self.amqp_url = amqp_url

    def set_stream(self, stream_id: str):
        self.queue = stream_id

    def publish_cloudevent(self, cloudevent, exchange=''):
        params = pika.URLParameters(self.amqp_url)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue=self.queue)
        channel.basic_publish(exchange='',
                              routing_key=self.queue,
                              body=cloudevent.MarshalJSON(json.dumps).read())
        connection.close()

    def get_json_eventsource(self):
        parameters = vars(self).copy()
        del parameters['name']
        return {'name': self.name, 'class': self.__class__.__name__, 'parameters': parameters}
