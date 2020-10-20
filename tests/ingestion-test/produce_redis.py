import platform
import sys
from uuid import uuid4
from datetime import datetime
from redis import StrictRedis
import requests


def produce_events(node, nodes, tstamp=None):
    red = StrictRedis(host='127.0.0.1', port='6379', password='potato')

    print('Producing events from node {} (nº nodes {})'.format(node, nodes))

    if tstamp is not None:
        while True:
            res = requests.get('http://worldtimeapi.org/api/ip')
            res_json = res.json()
            now = res_json['unixtime']
            if now >= tstamp:
                break

    print('Go')

    e0 = (1000 // nodes) * node
    e1 = (1000 // nodes) * (node + 1)
    if node == nodes-1:
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

            red.xadd('ingestion', event)


if __name__ == '__main__':
    try:
        node, nodes, tstamp = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
    except:
        node, nodes, tstamp = int(sys.argv[1]), int(sys.argv[2]), None

    produce_events(node, nodes, tstamp)
