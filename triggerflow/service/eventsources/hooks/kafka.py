import json
import logging
from enum import Enum
from typing import List
from threading import Thread
from multiprocessing import Queue, Value
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Consumer, TopicPartition

from ..model import EventSourceHook


class KafkaAuthMode(Enum):
    NONE = 0
    SASL_PLAINTEXT = 1


class KafkaEventSource(EventSourceHook):
    def __init__(self,
                 event_queue: Queue,
                 broker_list: List[str],
                 topic: str,
                 auth_mode: str,
                 username: str = None,
                 password: str = None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.event_queue = event_queue

        self.broker_list = broker_list
        self.username = username
        self.password = password
        self.auth_mode = auth_mode

        self.__config = {'bootstrap.servers': ','.join(self.broker_list),
                         'group.id': self.name,
                         'default.topic.config': {'auto.offset.reset': 'earliest'},
                         'enable.auto.commit': False
                         }

        if self.auth_mode == 'SASL_PLAINTEXT':
            # append Event streams specific config
            self.__config.update({'ssl.ca.location': '/etc/ssl/certs/',
                                  'sasl.mechanisms': 'PLAIN',
                                  'sasl.username': self.username,
                                  'sasl.password': self.password,
                                  'security.protocol': 'sasl_ssl'
                                  })

        self.consumer = None
        self.topic = topic or self.name
        self.records = []
        self.commit_queue = Queue()
        self.created_topic = Value('i', 0)

    def __start_commiter(self):
        def commiter(commit_queue):
            while True:
                ids = commit_queue.get()
                if ids is None:
                    break
                offsets = []
                for id in ids:
                    offsets.append(TopicPartition(*id))
                self.consumer.commit(offsets=offsets, asynchronous=False)

        self.__commiter = Thread(target=commiter, args=(self.commit_queue, ))
        self.__commiter.start()

    def run(self):
        self.consumer = Consumer(self.__config)
        self.__start_commiter()

        # Create topic if it does not exist
        topics = self.consumer.list_topics().topics
        if self.topic not in topics:
            self.__create_topic(self.topic)
            self.created_topic.value = 1

        self.consumer.subscribe([self.topic])
        logging.info("[{}] Started consuming from topic {}".format(self.name, self.topic))
        payload = None
        while True:
            try:
                message = self.consumer.poll()
                logging.info("[{}] Received event".format(self.name))
                payload = message.value().decode('utf-8')
                event = json.loads(payload)
                try:
                    event['data'] = json.loads(event['data'])
                except Exception:
                    pass
                event['id'] = (message.topic(), message.partition(), message.offset() + 1)
                event['event_source'] = self.name
                self.event_queue.put(event)
                self.records.append(message)
            except TypeError:
                logging.error("[{}] Received event did not contain "
                              "JSON payload, got {} instead".format(self.name, type(payload)))

    def commit(self, ids):
        self.commit_queue.put(ids)

    def __create_topic(self, topic):
        admin_client = AdminClient(self.__config)

        new_topic = [NewTopic(topic, num_partitions=1, replication_factor=1)]
        # Call create_topics to asynchronously create topics, a dict of <topic,future> is returned.
        fs = admin_client.create_topics(new_topic)

        # Wait for operation to finish.
        # Timeouts are preferably controlled by passing request_timeout=15.0
        # to the create_topics() call.
        # All futures will finish at the same time.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                logging.info("[{}] Topic {} created".format(self.name, topic))
                return True
            except Exception as e:
                logging.info("[{}] Failed to create topic {}: {}".format(self.name, topic, e))
                return False

    def __delete_topic(self, topic):
        admin_client = AdminClient(self.__config)

        # Call delete_topics to asynchronously delete topics, a dict of <topic,future> is returned.
        fs = admin_client.delete_topics([self.topic])

        # Wait for operation to finish.
        # Timeouts are preferably controlled by passing request_timeout=15.0
        # to the delete_topics() call.
        # All futures will finish at the same time.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                logging.info("[{}] Topic {} deleted".format(self.name, topic))
                return True
            except Exception as e:
                logging.info("[{}] Failed to deleted topic {}: {}".format(self.name, topic, e))
                return False

    @staticmethod
    def __get_offset_list(events):
        offsets = []
        for message in events:
            # Add one to the offset, otherwise we'll consume this message again.
            # That's just how Kafka works, you place the bookmark at the *next* message.
            offsets.append(TopicPartition(message.topic(), message.partition(), message.offset() + 1))

        return offsets

    @property
    def config(self):
        d = self.__config.copy()
        d['type'] = 'kafka'
        d['topic'] = self.topic
        return d

    def stop(self):
        logging.info("[{}] Stopping event source".format(self.name))
        self.commit_queue.pu(None)
        self.__commiter.join()
        if self.created_topic.value:
            self.__delete_topic(self.topic)
        self.terminate()
