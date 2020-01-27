import json
import logging
from uuid import uuid1
from enum import Enum
from typing import List
from multiprocessing import Queue

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Consumer, Producer, TopicPartition

from .model import Hook


class KafkaAuthMode(Enum):
    NONE = 0
    SASL_PLAINTEXT = 1


class KafkaCloudEventSourceHook(Hook):
    def __init__(self,
                 event_queue: Queue,
                 broker_list: List[str],
                 topic: str,
                 auth_mode: str,
                 username: str,
                 password: str,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.group_id = str(uuid1())
        self.event_queue = event_queue

        self.config = {'bootstrap.servers': ','.join(broker_list),
                       'group.id': self.group_id,
                       'default.topic.config': {'auto.offset.reset': 'earliest'},
                       'enable.auto.commit': False
                       }
        # append Event streams specific config
        self.config.update({'ssl.ca.location': '/etc/ssl/certs/',
                            'sasl.mechanisms': 'PLAIN',
                            'sasl.username': username,
                            'sasl.password': password,
                            'security.protocol': 'sasl_ssl'
                            })

        self.consumer = None
        self.topic = topic
        self.records = list()
        self.created_topic = False

    def run(self):
        self.consumer = Consumer(self.config)

        # Create topic if it does not exist
        topics = self.consumer.list_topics().topics
        if self.topic not in topics:
            self.__create_topic(self.topic)
            self.created_topic = True

        logging.info("[{}] Started consuming from topic {}".format(self.name, self.topic))
        self.consumer.subscribe([self.topic])
        payload = None
        while True:
            try:
                records = self.consumer.consume()
                for record in records:
                    logging.info("[{}] Received event".format(self.topic))
                    payload = record.value().decode('utf-8')
                    event = json.loads(payload)
                    self.event_queue.put(event)
                    self.records.append(record)
            except TypeError:
                logging.error("[{}] Received event did not contain "
                              "JSON payload, got {} instead".format(self.name, type(payload)))

    def poll(self):
        return self.consumer.poll(timeout=1.0)

    def body(self, record):
        return json.loads(record.value())

    def commit(self, records):
        self.consumer.commit(offsets=self.__get_offset_list(self.records), async=False)

    def stop(self):
        self.consumer.close()
        logging.info('[{}] Consumer closed for topic {}'.format(self.name, self.topic))

        if self.created_topic:
            try:
                admin_client = AdminClient(self.config)
                admin_client.delete_topics([self.topic])
                logging.info('[{}] Topic {} deleted'.format(self.name, self.topic))
            except Exception as e:
                logging.info("[{}] Failed to delete topic {}: {}".format(self.name, self.topic, e))
        self.terminate()

    def __create_topic(self, topic):
        admin_client = AdminClient(self.config)

        new_topic = [NewTopic(topic, num_partitions=3, replication_factor=3)]
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

    @staticmethod
    def __get_offset_list(events):
        offsets = []
        for message in events:
            # Add one to the offset, otherwise we'll consume this message again.
            # That's just how Kafka works, you place the bookmark at the *next* message.
            offsets.append(TopicPartition(message.topic(), message.partition(), message.offset() + 1))

        return offsets
