import json
import time

from confluent_kafka import Producer, Consumer

from eventprocessor_client.utils import load_config_yaml

if __name__ == '__main__':
    kafka_credentials = load_config_yaml('~/kafka_credentials.yaml')

    config = {'bootstrap.servers': ','.join(kafka_credentials['eventstreams']['kafka_brokers_sasl']),
              'ssl.ca.location': '/etc/ssl/certs/',
              'sasl.mechanisms': 'PLAIN',
              'sasl.username': kafka_credentials['eventstreams']['user'],
              'sasl.password': kafka_credentials['eventstreams']['password'],
              'security.protocol': 'sasl_ssl'
              }

    def delivery_callback(err, msg):
        if err:
            print('Failed delivery: {}'.format(err))
        else:
            print('Message delivered: {} {} {}'.format(msg.topic(), msg.partition(), msg.offset()))

    kafka_producer = Producer(**config)
    termination_event = {'subject': 'init__', 'type': 'termination.event.success'}
    kafka_producer.produce(topic='branch_b078e8f0-870b-41a7-917f-70f4cf6799fb', value=json.dumps(termination_event), callback=delivery_callback)
    kafka_producer.flush()

