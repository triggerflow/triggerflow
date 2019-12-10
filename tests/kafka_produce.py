import json

from confluent_kafka import Producer

from api.utils import load_config_yaml

if __name__ == '__main__':
    kafka_credentials = load_config_yaml('~/kafka_credentials.yaml')

    config = {'bootstrap.servers': ','.join(kafka_credentials['evenstreams']['kafka_brokers_sasl']),
              'ssl.ca.location': '/etc/ssl/certs/',
              'sasl.mechanisms': 'PLAIN',
              'sasl.username': kafka_credentials['evenstreams']['username'],
              'sasl.password': kafka_credentials['evenstreams']['password'],
              'security.protocol': 'sasl_ssl'
              }

    kafka_producer = Producer(config)
    termination_event = {'subject': 'init__'}
    kafka_producer.produce(topic='hello', value=json.dumps(termination_event))
    kafka_producer.flush()
