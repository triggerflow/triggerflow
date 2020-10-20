import json
import sys
import platform
from uuid import uuid4
from datetime import datetime
import pika
import requests


def produce_events(node, nodes, tstamp):
    queue = 'ingestion'
    url = 'amqp://guest:guest@127.0.0.1:5672'
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()  # start a channel
    channel.queue_declare(queue=queue)

    print('Producing events from node {} (nº nodes {})'.format(node, nodes))

    if tstamp:
        while True:
            res = requests.get('http://worldtimeapi.org/api/ip')
            res_json = res.json()
            now = res_json['unixtime']
            if now >= tstamp:
                break

    print('Go')

    e0 = (1000 // nodes) * node
    e1 = (1000 // nodes) * (node + 1)
    if node == nodes - 1:
        e1 += 1000 % nodes
    for _ in range(e0, e1):
        for i in range(200):
            uuid = uuid4()

            event = {'specversion': '1.0',
                     'id': uuid.hex,
                     'source': f'urn:{platform.node()}:{str(uuid)}',
                     'type': 'event.triggerflow.test',
                     'time': str(datetime.utcnow().isoformat("T") + "Z"),
                     'subject': 'join{}'.format(i)}
            channel.basic_publish(exchange='', routing_key=queue, body=json.dumps(event))


if __name__ == '__main__':
    try:
        node, nodes, tstamp = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
    except:
        node, nodes, tstamp = int(sys.argv[1]), int(sys.argv[2]), None

    produce_events(node, nodes, tstamp)
