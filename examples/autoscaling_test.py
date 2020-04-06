
import json
import time
from confluent_kafka import Producer
from concurrent.futures import ThreadPoolExecutor as Pool

TOPIC = 'pywren{}-kafka-eventsource'
active_workflows = {}


def publish_events():

    config = {'bootstrap.servers': '192.168.5.35:9092'}

    def delivery_callback(err, msg):
        return
        if err:
            print('Failed delivery: {}'.format(err))
        else:
            print('Message delivered: {} {} {}'.format(msg.topic(), msg.partition(), msg.offset()))

    def generate_events(i):
        global active_workflows
        n_events = 30
        active_workflows[i] = time.time()
        kafka_producer = Producer(**config)

        for _ in range(n_events):
            termination_event = {'source': 'kafka_test', 'subject': 'map_{}'.format(i),
                                 'type': 'termination.event.success', 'data': '"test"'}
            kafka_producer.produce(topic=TOPIC.format(i),
                                   value=json.dumps(termination_event),
                                   callback=delivery_callback)
            kafka_producer.flush()
            time.sleep(5)

        if i >= 30:
            del active_workflows[i]
            return

        time.sleep(60)

        for _ in range(n_events):
            termination_event = {'source': 'kafka_test', 'subject': 'map_{}'.format(i),
                                 'type': 'termination.event.success', 'data': '"test"'}
            kafka_producer.produce(topic=TOPIC.format(i),
                                   value=json.dumps(termination_event),
                                   callback=delivery_callback)
            kafka_producer.flush()
            time.sleep(3)

        del active_workflows[i]

    def generate_events_2(i):
        global active_workflows
        n_events = 10
        active_workflows[i] = time.time()
        kafka_producer = Producer(**config)

        for _ in range(n_events):
            termination_event = {'source': 'kafka_test', 'subject': 'map_{}'.format(i),
                                 'type': 'termination.event.success', 'data': '"test"'}
            kafka_producer.produce(topic=TOPIC.format(i),
                                   value=json.dumps(termination_event),
                                   callback=delivery_callback)
            kafka_producer.flush()
            time.sleep(5)

        if i > 55:
            del active_workflows[i]
            return

        time.sleep(30)

        for _ in range(n_events):
            termination_event = {'source': 'kafka_test', 'subject': 'map_{}'.format(i),
                                 'type': 'termination.event.success', 'data': '"test"'}
            kafka_producer.produce(topic=TOPIC.format(i),
                                   value=json.dumps(termination_event),
                                   callback=delivery_callback)
            kafka_producer.flush()
            time.sleep(5)

        del active_workflows[i]

    def generate_events_3(i):
        global active_workflows
        n_events = 20
        active_workflows[i] = time.time()
        kafka_producer = Producer(**config)

        for _ in range(n_events):
            termination_event = {'source': 'kafka_test', 'subject': 'map_{}'.format(i),
                                 'type': 'termination.event.success', 'data': '"test"'}
            kafka_producer.produce(topic=TOPIC.format(i),
                                   value=json.dumps(termination_event),
                                   callback=delivery_callback)
            kafka_producer.flush()
            time.sleep(5)

        time.sleep(44)

        for _ in range(n_events):
            termination_event = {'source': 'kafka_test', 'subject': 'map_{}'.format(i),
                                 'type': 'termination.event.success', 'data': '"test"'}
            kafka_producer.produce(topic=TOPIC.format(i),
                                   value=json.dumps(termination_event),
                                   callback=delivery_callback)
            kafka_producer.flush()
            time.sleep(5)

        del active_workflows[i]

    def monitor():
        while True:
            print("[{}, {}],".format(int(time.time()), len(active_workflows)))
            time.sleep(1)

    with Pool(max_workers=128) as executor:
        executor.submit(monitor)
        for i in range(50):
            executor.submit(generate_events, i)
            time.sleep(0.5)

        time.sleep(100)

        for i in range(35, 85):
            executor.submit(generate_events_2, i)
            time.sleep(0.3)

        time.sleep(70)

        for i in range(85, 99):
            executor.submit(generate_events_3, i)
            time.sleep(0.3)


if __name__ == '__main__':
    publish_events()
