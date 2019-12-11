from api.client import CloudEventProcessorClient, DefaultActions, DefaultConditions
from api.utils import load_config_yaml
from api.sources.kafka import KafkaCloudEventSource, KafkaSASLAuthMode

if __name__ == "__main__":
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

    # [map1-1 ... map1-10] >> [map2-1 ... map2-10] >> [map3-1 ... map3-10]

    for i in range(10):
        er.add_trigger(kafka.event('init__'),
                       condition=DefaultConditions.IBM_CF_JOIN,
                       action=DefaultActions.SIMULATE_CF_INVOKE,
                       context={'subject': 'map1-{}'.format(i), 'args': [{'x': x} for x in range(100)], 'kind': 'map'})

    for i in range(10):
        er.add_trigger([kafka.event('map1-{}'.format(x)) for x in range(10)],
                       condition=DefaultConditions.IBM_CF_JOIN,
                       action=DefaultActions.SIMULATE_CF_INVOKE,
                       context={'subject': 'map2-{}'.format(i), 'args': [{'x': x} for x in range(100)], 'kind': 'map'})

    for i in range(10):
        er.add_trigger([kafka.event('map2-{}'.format(x)) for x in range(10)],
                       condition=DefaultConditions.IBM_CF_JOIN,
                       action=DefaultActions.SIMULATE_CF_INVOKE,
                       context={'subject': 'map3-{}'.format(i), 'args': [{'x': x} for x in range(100)], 'kind': 'map'})

    er.add_trigger([kafka.event('map3-{}'.format(x)) for x in range(10)],
                   condition=DefaultConditions.IBM_CF_JOIN,
                   action=DefaultActions.TERMINATE)
