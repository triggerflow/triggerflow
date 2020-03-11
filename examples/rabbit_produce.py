import pika
import json

from eventprocessor_client.utils import load_config_yaml

if __name__ == '__main__':
    rabbit_credentials = load_config_yaml('~/rabbit_credentials.yaml')
    params = pika.URLParameters(rabbit_credentials['rabbitmq']['amqp_url'])
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue='hello')
    termination_event = {'subject': 'init__'}
    channel.basic_publish(exchange='', routing_key='hello', body=json.dumps(termination_event))
    connection.close()
