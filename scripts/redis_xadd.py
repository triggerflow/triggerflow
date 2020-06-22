import redis
import sys
from uuid import uuid4
from platform import node
from datetime import datetime

sys.path.append('../')

red = redis.client.StrictRedis(host='', port=6379, password="", db=0)

uuid = uuid4()
event = {'specversion': '1.0',
         'id': uuid.hex,
         'source': f'urn:{node()}:{str(uuid)}',
         'type': 'event.triggerflow.init',
         'time': str(datetime.utcnow().isoformat("T") + "Z"),
         'subject': '__init__'}

red.xadd('WORKSPACE', event)
