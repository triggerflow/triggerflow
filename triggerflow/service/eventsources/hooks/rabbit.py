import json
import pika
import logging
from multiprocessing import Queue

from ..model import EventSourceHook


class RabbitEventSource(EventSourceHook):
    def __init__(self,
                 event_queue: Queue,
                 amqp_url: str,
                 queue: str,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.event_queue = event_queue
        self.queue = queue or self.name
        self.amqp_url = amqp_url

        self.params = pika.URLParameters(self.amqp_url)
        connection = pika.BlockingConnection(self.params)
        self.channel = connection.channel()
        self.channel.queue_declare(queue=self.queue)

    def run(self):
        def callback(ch, method, properties, body):
            logging.info("[{}] Received event".format(self.name))
            event = json.loads(body)
            event['data'] = json.loads(event['data'])
            self.event_queue.put(event)

        self.channel.basic_consume(self.queue, callback, auto_ack=False)

    def commit(self, records):
        for record in records:
            method_frame, header_frame, body = record
            self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    def stop(self):
        self.channel.stop_consuming()
        self.channel.queue_delete(queue=self.queue)
        self.terminate()

    @property
    def config(self):
        d = {'type': 'rabbit', 'amqp_url': self.amqp_url, 'queue': self.queue}
        return d
