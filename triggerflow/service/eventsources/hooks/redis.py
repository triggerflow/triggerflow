import json
from multiprocessing import Queue

import pika

from ..model import EventSourceHook


class RedisEventSource(EventSourceHook):
    def __init__(self,
                 event_queue: Queue,
                 amqp_url: str,
                 topic: str,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.event_queue = event_queue
        self.topic = topic
        self.params = pika.URLParameters(amqp_url)
        self.channel = None

    def run(self):
        connection = pika.BlockingConnection(self.params)
        self.channel = connection.channel()
        self.channel.queue_declare(queue=self.topic)

        def callback(ch, method, properties, body):
            self.event_queue.put(json.loads(body))

        self.channel.basic_consume(self.topic, callback, auto_ack=False)

    def poll(self):
        method_frame, header_frame, body = self.channel.basic_get(queue=self.topic)
        if method_frame is None:
            return None
        else:
            return method_frame, header_frame, body

    def body(self, record):
        method_frame, header_frame, body = record
        return json.loads(body)

    def commit(self, records):
        for record in records:
            method_frame, header_frame, body = record
            self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    def stop(self):
        self.channel.stop_consuming()
