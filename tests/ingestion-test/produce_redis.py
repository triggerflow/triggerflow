import json
import sys
import platform
from uuid import uuid4
from datetime import datetime
from redis import StrictRedis

TOPIC = 'pywren'


def produce_events(node, nodes):
    red = StrictRedis(host='127.0.0.1', port=6379, password="hello", db=0)

    if 1000 % nodes != 0:
        print('1000 is not divisible by {}'.format(nodes))
        return

    print('Producing events from node {} (nº nodes {})'.format(node, nodes))

    for _ in range((1000 // nodes) * node - 1, (1000 // nodes) * node):
        for i in range(200):
            uuid = uuid4()

            event = {'specversion': '1.0',
                     'id': uuid.hex,
                     'source': f'urn:{platform.node()}:{str(uuid)}',
                     'type': 'event.triggerflow.test',
                     'time': str(datetime.utcnow().isoformat("T") + "Z"),
                     'subject': 'join{}'.format(i)}

            red.xadd(TOPIC, event)


if __name__ == '__main__':
    try:
        node, nodes = int(sys.argv[1]), int(sys.argv[2])
    except Exception:
        node, nodes = 1, 1

    produce_events(node, nodes)
