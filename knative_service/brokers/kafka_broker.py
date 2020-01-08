import json
import logging
from uuid import uuid1
from enum import Enum
from typing import List

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Consumer, TopicPartition

from .broker import Broker


class KafkaSASLAuthMode(Enum):
    SASL_PLAINTEXT = 0


class KafkaBroker(Broker):
    def __init__(self, broker_list: List[str], topic: str, username: str, password: str):
        super().__init__()
        self.group_id = str(uuid1())

        self.config = {'bootstrap.servers': ','.join(broker_list),
                       'group.id': self.group_id,
                       'default.topic.config': {'auto.offset.reset': 'earliest'},
                       'enable.auto.commit': False
                       }
        # append Event streams specific config
        #self.config.update({'ssl.ca.location': '/etc/ssl/certs/',
        #                    'sasl.mechanisms': 'PLAIN',
        #                    'sasl.username': username,
        #                    'sasl.password': password,
        #                    'security.protocol': 'sasl_ssl'
        #                   })
        print('++++')
        self.consumer = Consumer(self.config)
        print('++---------++')
        # Create topic if it does not exist
        #topics = self.consumer.list_topics().topics
        ###print(topui)
        #if topic not in topics:
        #    self.__create_topic(topic)

        #self.consumer.subscribe([topic])

    def poll(self):
        return self.consumer.poll(timeout=1.0)

    def body(self, record):
        return json.loads(record.value())

    def commit(self, record):
        self.consumer.commit(offsets=self.__get_offset_list(record), async=False)

    def __create_topic(self, topic):
        """
        Create topics
        """
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
                logging.info("Topic {} created".format(topic))
                return True
            except Exception as e:
                logging.info("Failed to create topic {}: {}".format(topic, e))
                return False

    @staticmethod
    def __get_offset_list(events):
        offsets = []
        for message in events:
            # Add one to the offset, otherwise we'll consume this message again.
            # That's just how Kafka works, you place the bookmark at the *next* message.
            offsets.append(TopicPartition(message.topic(), message.partition(), message.offset() + 1))

        return offsets
