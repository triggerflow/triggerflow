import json
import time

from confluent_kafka import Producer, Consumer

from eventprocessor_client.utils import load_config_yaml

if __name__ == '__main__':
    kafka_credentials = load_config_yaml('~/client_config.yaml')['event_sources']['kafka']

    config = {'bootstrap.servers': ','.join(kafka_credentials['broker_list']),
              # 'ssl.ca.location': '/etc/ssl/certs/',
              # 'sasl.mechanisms': 'PLAIN',
              # 'sasl.username': kafka_credentials['user'],
              # 'sasl.password': kafka_credentials['password'],
              # 'security.protocol': 'sasl_ssl'
              }

    def delivery_callback(err, msg):
        if err:
            print('Failed delivery: {}'.format(err))
        else:
            print('Message delivered: {} {} {}'.format(msg.topic(), msg.partition(), msg.offset()))

    kafka_producer = Producer(**config)
    termination_event = {'subject': 'init__', 'type': 'termination.event.success'}
    kafka_producer.produce(topic='sequence_47b6d971-5756-4f89-9d52-2df59e9cdb85', value=json.dumps(termination_event), callback=delivery_callback)
    kafka_producer.flush()

