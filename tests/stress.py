import json
import time

from confluent_kafka import Producer

from api.client import CloudEventProcessorClient, DefaultActions, DefaultConditions
from api.utils import load_config_yaml
from api.sources.kafka import KafkaCloudEventSource, KafkaSASLAuthMode

n_steps = 1
n_maps = 50
n_funcs_per_map = 1000
triggers = []


def create_triggers():
    client_config = load_config_yaml('~/event-processor_credentials.yaml')
    kafka_credentials = load_config_yaml('~/kafka_credentials.yaml')

    kafka = KafkaCloudEventSource(broker_list=kafka_credentials['eventstreams']['kafka_brokers_sasl'],
                                  topic='stress_test',
                                  auth_mode=KafkaSASLAuthMode.SASL_PLAINTEXT,
                                  username=kafka_credentials['eventstreams']['user'],
                                  password=kafka_credentials['eventstreams']['password'])

    er = CloudEventProcessorClient(namespace='stress_test',
                                   event_source=kafka,
                                   global_context={
                                       'ibm_cf_credentials': client_config['authentication']['ibm_cf_credentials'],
                                       'kafka_credentials': kafka_credentials['eventstreams']},
                                   api_endpoint=client_config['event_processor']['api_endpoint'],
                                   authentication=client_config['authentication'])

    for i in range(n_maps):
        trg = er.add_trigger(kafka.event('init__'),
                             condition=DefaultConditions.TRUE,
                             action=DefaultActions.SIM_CF_INVOKE,
                             context={'subject': 'map0-{}'.format(i),
                                      'args': [{'x': x} for x in range(n_funcs_per_map)],
                                      'kind': 'map'})
        triggers.append(trg['trigger_id'])

    for step in range(1, n_steps):
        for i in range(n_maps):
            trg = er.add_trigger(kafka.event('map{}-{}'.format(step - 1, i)),
                                 condition=DefaultConditions.IBM_CF_JOIN,
                                 action=DefaultActions.SIM_CF_INVOKE,
                                 context={'subject': 'map{}-{}'.format(step, i),
                                          'args': [{'x': x} for x in range(n_funcs_per_map)], 'kind': 'map'})
            triggers.append(trg['trigger_id'])

    trg = er.add_trigger([kafka.event('map{}-{}'.format(n_steps - 1, x)) for x in range(n_maps)],
                         condition=DefaultConditions.IBM_CF_JOIN,
                         action=DefaultActions.TERMINATE)
    triggers.append(trg['trigger_id'])

    with open('triggers.txt', 'w') as txt:
        txt.write(str(triggers))


def generate_events():
    kafka_credentials = load_config_yaml('~/kafka_credentials.yaml')

    config = {'bootstrap.servers': ','.join(kafka_credentials['eventstreams']['kafka_brokers_sasl']),
              'ssl.ca.location': '/etc/ssl/certs/',
              'sasl.mechanisms': 'PLAIN',
              'sasl.username': kafka_credentials['eventstreams']['user'],
              'sasl.password': kafka_credentials['eventstreams']['password'],
              'security.protocol': 'sasl_ssl'
              }

    kafka_producer = Producer(**config)
    termination_event = {'subject': 'init__'}
    kafka_producer.produce(topic='stress_test', value=json.dumps(termination_event))
    kafka_producer.flush()

    time.sleep(0.05)

    for step in range(n_steps):
        for i in range(n_maps):
            trg = triggers.pop(0)
            for _ in range(n_funcs_per_map):
                kafka_producer.produce(topic='stress_test', value=json.dumps({'subject': 'map{}-{}'.format(step, i),
                                                                              'triggersource': trg}))
            kafka_producer.flush()
        time.sleep(0.05)


if __name__ == '__main__':
    create_triggers()
    # print(triggers)
    # time.sleep(15)
    # generate_events()
