import json
import time

from confluent_kafka import Producer, Consumer

from eventprocessor_client.utils import load_config_yaml

if __name__ == '__main__':
    kafka_credentials = load_config_yaml('~/client_config.yaml')['event_sources']['kafka']

    config = {'bootstrap.servers': ','.join(kafka_credentials['kafka_brokers_sasl']),
              'ssl.ca.location': '/etc/ssl/certs/',
              'sasl.mechanisms': 'PLAIN',
              'sasl.username': kafka_credentials['user'],
              'sasl.password': kafka_credentials['password'],
              'security.protocol': 'sasl_ssl'
              }

    def delivery_callback(err, msg):
        if err:
            print('Failed delivery: {}'.format(err))
        else:
            print('Message delivered: {} {} {}'.format(msg.topic(), msg.partition(), msg.offset()))

    kafka_producer = Producer(**config)
    termination_event = {'subject': 'init__', 'type': 'termination.event.success'}
    kafka_producer.produce(topic='parallel_2ad799fc-f51c-472b-a0d9-6958fda3417a', value=json.dumps(termination_event), callback=delivery_callback)
    kafka_producer.flush()

