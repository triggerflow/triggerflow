import json
import pika
import logging
from multiprocessing import Queue
from threading import Thread

from .model import EventSourceHook

logging.getLogger('pika').setLevel(logging.CRITICAL)


class RabbitMQEventSource(EventSourceHook):
    def __init__(self,
                 event_queue: Queue,
                 amqp_url: str,
                 queue: str,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.event_queue = event_queue
        self.queue = queue or self.name
        self.amqp_url = amqp_url
        self.params = None
        self.channel = None
        self.commit_queue = Queue()
        self.records = {}

    def __start_commiter(self):
        def commiter(commit_queue):
            while True:
                ids = commit_queue.get()
                if ids is None:
                    break
                for id in ids:
                    delivery_tag = self.records[id]
                    logging.debug('ACK of message with delivery tag {}'.format(delivery_tag))
                    self.channel.basic_ack(delivery_tag=delivery_tag)

        self.__commiter = Thread(target=commiter, args=(self.commit_queue,))
        self.__commiter.start()

    def run(self):
        self.params = pika.URLParameters(self.amqp_url)
        connection = pika.BlockingConnection(self.params)
        self.channel = connection.channel()
        self.channel.queue_declare(queue=self.queue)

        self.__start_commiter()

        while True:
            method_frame, header_frame, body = self.channel.basic_get(queue=self.queue)
            if None in {method_frame, header_frame, body}:
                continue
            try:
                event = json.loads(body)
                logging.debug("[{}] Received event".format(self.name))

                if not {'id', 'source', 'subject', 'type'}.issubset(set(event)):
                    raise Exception('Invalid Cloudevent')

                try:
                    event['data'] = json.loads(event['data'])
                except:
                    pass

                self.event_queue.put(event)
                self.records[event['id']] = method_frame.delivery_tag
            except Exception as e:
                logging.warning('[{}] Error while decoding event: {}'.format(self.name, e))

    def commit(self, ids):
        self.commit_queue.put(ids)

    def stop(self):
        self.channel.stop_consuming()
        self.channel.queue_delete(queue=self.queue)
        self.terminate()

    @property
    def config(self):
        d = {'type': 'rabbit', 'amqp_url': self.amqp_url, 'queue': self.queue}
        return d
