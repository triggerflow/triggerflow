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

    er = CloudEventProcessorClient(namespace='ibm_cf_test',
                                   event_source=kafka,
                                   default_context=client_config['authentication']['ibm_cf_credentials'],
                                   api_endpoint=client_config['event_processor']['api_endpoint'],
                                   authentication=client_config['authentication'])

    fqfn = 'https://us-east.functions.cloud.ibm.com/api/v1/namespaces/cloudlab_urv_us_east/actions/dag_test/sleep_rand'
    er.add_trigger(kafka.event('init__'),
                   action=DefaultActions.IBM_CF_INVOKE,
                   context={'subject': 'ca1', 'fqfn': fqfn, 'args': {'iter': 1}})

    er.add_trigger(kafka.event('t1'),
                   condition=DefaultConditions.JOIN,
                   action=DefaultActions.IBM_CF_INVOKE,
                   context={'subject': 'map1', 'fqfn': fqfn, 'args': [{'iter': 1}, {'iter': 2}, {'iter': 3}]})
    er.add_trigger(kafka.event('t1'),
                   condition=DefaultConditions.JOIN,
                   action=DefaultActions.IBM_CF_INVOKE,
                   context={'subject': 'ca2', 'fqfn': fqfn, 'args': {'iter': 1}})

    er.add_trigger([kafka.event('map1'), kafka.event('ca2')],
                   condition=DefaultConditions.JOIN,
                   action=DefaultActions.IBM_CF_INVOKE,
                   context={'subject': 'map2', 'fqfn': fqfn, 'args': [{'iter': 1}, {'iter': 2}]})

    er.add_trigger(kafka.event('map2'),
                   condition=DefaultConditions.JOIN,
                   action=DefaultActions.IBM_CF_INVOKE,
                   context={'subject': 'ca3', 'fqfn': fqfn, 'args': {'iter': 1}})

    er.add_trigger(kafka.event('ca3'),
                   condition=DefaultConditions.JOIN,
                   action=DefaultActions.TERMINATE)
