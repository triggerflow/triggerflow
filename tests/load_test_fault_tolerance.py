import json
from confluent_kafka import Producer

from triggerflow import TriggerflowClient, CloudEvent
from triggerflow.client import DefaultActions, DefaultConditions
from triggerflow.cache import get_triggerflow_config
from triggerflow.client.sources import KafkaEventSource


N_JOIN = [250, 500, 750, 1000, 1250, 1500]
N_STEPS = len(N_JOIN)
N_MAPS = 20
TOPIC = 'stress_kafka'


def setup():
    client_config = get_triggerflow_config('~/client_config.yaml')
    kafka_config = client_config['event_sources']['kafka']

    tf = TriggerflowClient(**client_config['triggerflow'], caching=True)

    kafka = KafkaEventSource(name='stress_kafka',
                             broker_list=kafka_config['broker_list'],
                             topic='stress_kafka')

    # tf.create_workspace(workspace='stress_kafka', event_source=kafka)
    tf.target_workspace(workspace='stress_kafka')

    for i in range(N_STEPS):
        for j in range(N_MAPS):
            tf.add_trigger(CloudEvent('map_{}_{}'.format(i, j)),
                           action=DefaultActions.PASS,
                           condition=DefaultConditions.SIMPLE_JOIN,
                           context={'total_activations': N_JOIN[i]})

    tf.commit_cached_triggers()


def publish_events():
    kafka_credentials = get_triggerflow_config('~/client_config.yaml')['event_sources']['kafka']

    config = {'bootstrap.servers': ','.join(kafka_credentials['broker_list'])}

    def delivery_callback(err, msg):
        if err:
            print('Failed delivery: {}'.format(err))
        else:
            print('Message delivered: {} {} {}'.format(msg.topic(), msg.partition(), msg.offset()))

    kafka_producer = Producer(**config)
    for i in range(N_STEPS):
        for j in range(N_MAPS):
            for _ in range(N_JOIN[i]):
                termination_event = {'source': 'test',
                                     'subject': 'map_{}_{}'.format(i, j),
                                     'type': 'termination.event.success',
                                     'datacontenttype': 'application/json',
                                     'data': {'id': 'map_{}_{}'.format(i, j)}}
                kafka_producer.produce(topic=TOPIC,
                                       value=json.dumps(termination_event),
                                       callback=delivery_callback)
            kafka_producer.flush()


if __name__ == '__main__':
    setup()
    # publish_events()
