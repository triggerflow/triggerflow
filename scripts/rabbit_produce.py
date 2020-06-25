import pika
import json

if __name__ == '__main__':
    params = pika.URLParameters('')
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue='hello')
    termination_event = {'subject': 'init__'}
    channel.basic_publish(exchange='', routing_key='hello', body=json.dumps(termination_event))
    connection.close()
