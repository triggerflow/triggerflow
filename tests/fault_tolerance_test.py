import json
import time
from confluent_kafka import Producer
from triggerflow import TriggerflowClient, CloudEvent
from triggerflow.functions import DefaultActions, DefaultConditions
from triggerflow.cache import get_triggerflow_config
from triggerflow.client.sources import KafkaEventSource


N_MAPS = 3
N_JOIN = 15
TOPIC = 'fail_test'


def setup():
    client_config = get_triggerflow_config('~/client_config.yaml')
    kafka_config = client_config['event_sources']['kafka']

    ep = TriggerflowClient(**client_config['triggerflow'])

    kafka = KafkaEventSource(name=TOPIC,
                             broker_list=kafka_config['broker_list'],
                             topic=TOPIC)

    # ep.create_workspace(workspace='fail_test', event_source=kafka)
    ep.target_workspace('fail_test')

    for i in range(N_MAPS):
        ep.add_trigger(CloudEvent('map_{}'.format(i)),
                       action=DefaultActions.PASS,
                       condition=DefaultConditions.SIMPLE_JOIN,
                       context={'total_activations': N_JOIN})


def publish_events():
    kafka_credentials = get_triggerflow_config('~/client_config.yaml')['event_sources']['kafka']

    config = {'bootstrap.servers': ','.join(kafka_credentials['broker_list'])}

    def delivery_callback(err, msg):
        if err:
            print('Failed delivery: {}'.format(err))
        else:
            print('Message delivered: {} {} {}'.format(msg.topic(), msg.partition(), msg.offset()))

    kafka_producer = Producer(**config)
    for i in range(N_MAPS):
        for j in range(N_JOIN):
            termination_event = {'source': 'test',
                                 'subject': 'map_{}'.format(i),
                                 'type': 'termination.event.success',
                                 'datacontenttype': 'application/json',
                                 'data': {'num': j}}
            kafka_producer.produce(topic=TOPIC,
                                   value=json.dumps(termination_event),
                                   callback=delivery_callback)
            kafka_producer.flush()
            time.sleep(1)
        time.sleep(10)


if __name__ == '__main__':
    # setup()
    publish_events()
