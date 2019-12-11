import logging
import time
import uuid

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Consumer


class KafkaClient:

    def __init__(self, brokers: list, username: str, password: str):
        self.brokers = brokers
        self.username = username
        self.password = password
        self.group_id = str(uuid.uuid1())

        self.config = {'bootstrap.servers': ','.join(self.brokers),
                       'group.id': self.group_id,
                       'default.topic.config': {'auto.offset.reset': 'earliest'},
                       'enable.auto.commit': False
                       }

        # append Event streams specific config
        self.config.update({'ssl.ca.location': '/etc/ssl/certs/',
                            'sasl.mechanisms': 'PLAIN',
                            'sasl.username': self.username,
                            'sasl.password': self.password,
                            'security.protocol': 'sasl_ssl'
                            })

    def topic_exists(self, topic):
        cluster_metadata = Consumer(self.config).list_topics()
        return topic in cluster_metadata.topics

    def create_consumer(self, topic):
        """
        Creates a new kafka consumer
        """
        consumer = Consumer(self.config)
        assigned = False

        def assignation(consumer, partitions):
            logging.info('Consumer assigned to partition: {}'.format(partitions))

        consumer.subscribe([topic], on_assign=assignation)
        logging.info("Subscribed to topic: {}".format(topic))
        return consumer

    def create_topic(self, topic):
        """
        Create topics
        """
        admin_client = AdminClient(self.config)

        new_topic = [NewTopic(topic, num_partitions=3, replication_factor=3)]
        # Call create_topics to asynchronously create topics, a dict
        # of <topic,future> is returned.
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

    def delete_topic(self, topic):
        """
        delete a topic
        """
        # Call delete_topics to asynchronously delete topics, a future is returned.
        # By default this operation on the broker returns immediately while
        # topics are deleted in the background. But here we give it some time (30s)
        # to propagate in the cluster before returning.
        #
        # Returns a dict of <topic,future>.
        admin_client = AdminClient(self.config)
        fs = admin_client.delete_topics([topic], operation_timeout=30)

        # Wait for operation to finish.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                logging.info("Topic {} deleted".format(topic))
            except Exception as e:
                logging.info("Failed to delete topic {}: {}".format(topic, e))


def print_assignment(consumer, partitions):
    print('Assigned consumer: {}'.format(partitions))
