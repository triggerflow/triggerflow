import time

from redis import StrictRedis

TOPIC = 'pywren'


def consume_events():

    red = StrictRedis(host='127.0.0.1', port=6379, password="hello", db=0)
    start = None
    total = 0
    while True:
        try:
            records = red.xread({TOPIC: 0}, block=0)[0][1]
            start = time.time() if not start else start
            total = total+len(records)
            print("[{}] Received event {}".format(TOPIC, total))
            end = time.time()
        except Exception as e:
            break

    print('Total time: {}'.format(end-start))


if __name__ == '__main__':
    consume_events()
