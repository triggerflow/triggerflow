import json
import pika

from .broker import Broker


class RabbitMQBroker(Broker):
    def __init__(self, amqp_url: str, topic: str):
        super().__init__()
        self.topic = topic
        params = pika.URLParameters(amqp_url)
        connection = pika.BlockingConnection(params)
        self.channel = connection.channel()
        self.channel.queue_declare(queue=topic)

    def poll(self):
        method_frame, header_frame, body = self.channel.basic_get(queue=self.topic)
        if method_frame is None:
            return None
        else:
            return method_frame, header_frame, body

    def body(self, record):
        method_frame, header_frame, body = record
        return json.loads(body)

    def commit(self, record):
        method_frame, header_frame, body = record
        self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)
