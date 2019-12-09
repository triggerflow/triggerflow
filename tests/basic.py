from api.client import CloudEventProcessorClient
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

    er = CloudEventProcessorClient(namespace='test',
                                   event_source=kafka,
                                   default_context={'my_things': 'hi'},
                                   api_endpoint=client_config['event_processor']['api_endpoint'],
                                   ibm_cf_credentials=client_config['ibm_cf_credentials'])

    er.add_trigger(kafka.event('t1', 'termination.event.success'), context={'some_important_things': 'hi'})
