from confluent_kafka import Consumer
from triggerflow.client.utils import load_config_yaml

if __name__ == '__main__':
    kafka_credentials = load_config_yaml('~/kafka_credentials.yaml')

    config = {'bootstrap.servers': ','.join(kafka_credentials['eventstreams']['kafka_brokers_sasl']),
              'ssl.ca.location': '/etc/ssl/certs/',
              'sasl.mechanisms': 'PLAIN',
              'sasl.username': kafka_credentials['eventstreams']['user'],
              'sasl.password': kafka_credentials['eventstreams']['password'],
              'security.protocol': 'sasl_ssl',
              'group.id': '123',
              'default.topic.config': {'auto.offset.reset': 'earliest'},
              'enable.auto.commit': False
              }


    def print_assignment(consumer, partitions):
        print('Assignment: {}'.format(partitions))

    kafka_consumer = Consumer(config)
    kafka_consumer.subscribe(['hello'], on_assign=print_assignment)
    print('consuming')
    while True:
        msg = kafka_consumer.poll(timeout=1.0)
        if msg is None:
            print('null message')
        else:
            print(msg.value())
