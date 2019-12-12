from api.client import CloudEventProcessorClient, DefaultActions, DefaultConditions
from api.utils import load_config_yaml
from api.sources.kafka import KafkaCloudEventSource, KafkaSASLAuthMode

if __name__ == "__main__":
    client_config = load_config_yaml('~/event-processor_credentials.yaml')
    kafka_credentials = load_config_yaml('~/kafka_credentials.yaml')

    kafka = KafkaCloudEventSource(broker_list=kafka_credentials['eventstreams']['kafka_brokers_sasl'],
                                  topic='hello',
                                  auth_mode=KafkaSASLAuthMode.SASL_PLAINTEXT,
                                  username=kafka_credentials['eventstreams']['user'],
                                  password=kafka_credentials['eventstreams']['password'])

    er = CloudEventProcessorClient(namespace='ibm_cf_test_kafka',
                                   event_source=kafka,
                                   global_context={
                                       'ibm_cf_credentials': client_config['authentication']['ibm_cf_credentials'],
                                       'kafka_credentials': kafka_credentials['eventstreams']},
                                   api_endpoint=client_config['event_processor']['api_endpoint'],
                                   authentication=client_config['authentication'])

    # init__ >> ca1 >> [map1, ca2] >> map2 >> ca3 >> end__

    url = 'https://us-east.functions.cloud.ibm.com/api/v1/namespaces/cloudlab_urv_us_east/actions/eventprocessor_functions/kafka_test'
    er.add_trigger(kafka.event('init__'),
                   action=DefaultActions.IBM_CF_INVOKE_KAFKA,
                   context={'subject': 'ca1', 'url': url, 'args': {'iter': 1}, 'kind': 'callasync'})

    er.add_trigger(kafka.event('ca1'),
                   condition=DefaultConditions.IBM_CF_JOIN,
                   action=DefaultActions.IBM_CF_INVOKE_KAFKA,
                   context={'subject': 'map1',
                            'url': url,
                            'args': [{'iter': 1}, {'iter': 2}, {'iter': 3}],
                            'kind': 'map'})
    er.add_trigger(kafka.event('ca1'),
                   condition=DefaultConditions.IBM_CF_JOIN,
                   action=DefaultActions.IBM_CF_INVOKE_KAFKA,
                   context={'subject': 'ca2', 'url': url, 'args': {'iter': 1}, 'kind': 'callasync'})

    er.add_trigger([kafka.event('map1'), kafka.event('ca2')],
                   condition=DefaultConditions.IBM_CF_JOIN,
                   action=DefaultActions.IBM_CF_INVOKE_KAFKA,
                   context={'subject': 'map2', 'url': url, 'args': [{'iter': 1}, {'iter': 2}], 'kind': 'map'})

    er.add_trigger(kafka.event('map2'),
                   condition=DefaultConditions.IBM_CF_JOIN,
                   action=DefaultActions.IBM_CF_INVOKE_KAFKA,
                   context={'subject': 'ca3', 'url': url, 'args': {'iter': 1}, 'kind': 'callasync'})

    er.add_trigger(kafka.event('ca3'),
                   condition=DefaultConditions.IBM_CF_JOIN,
                   action=DefaultActions.TERMINATE)
