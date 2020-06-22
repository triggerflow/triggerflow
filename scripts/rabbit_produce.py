import pika
import json
from triggerflow.cache import get_triggerflow_config

if __name__ == '__main__':
    rabbit_credentials = get_triggerflow_config('~/rabbit_credentials.yaml')
    params = pika.URLParameters(rabbit_credentials['rabbitmq']['amqp_url'])
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue='hello')
    termination_event = {'subject': 'init__'}
    channel.basic_publish(exchange='', routing_key='hello', body=json.dumps(termination_event))
    connection.close()
