import json
import time
from confluent_kafka import Consumer


TOPIC = 'pywren'


def consume_events():

    config = {'bootstrap.servers': '192.168.2.51:9092',
              'group.id': 'test-group',
              'default.topic.config': {'auto.offset.reset': 'earliest'},
              'enable.auto.commit': False
              }
    consumer = Consumer(config)
    consumer.subscribe([TOPIC])
    start = None
    total = 0
    while True:
        try:
            message = consumer.poll(timeout=10)
            start = time.time() if not start else start
            payload = message.value().decode('utf-8')
            event = json.loads(payload)
            total = total+1
            print("[{}] Received event {}".format(TOPIC, total))
            end = time.time()
        except Exception as e:
            break

    print('Total time: {}'.format(end-start))


if __name__ == '__main__':
    consume_events()
