from eventprocessor_client import CloudEventProcessorClient
from eventprocessor_client.utils import load_config_yaml
from eventprocessor_client.sources.interfaces.kafka import KafkaCloudEventSource, KafkaAuthMode

if __name__ == "__main__":
    client_config = load_config_yaml('~/client_config.yaml')
    kafka_config = client_config['event_sources']['kafka']

    er = CloudEventProcessorClient(**client_config['event_processor'])

    kafka = KafkaCloudEventSource(name='my_kafka_eventsource',
                                  broker_list=kafka_config['kafka_brokers_sasl'],
                                  topic='hello',
                                  auth_mode=KafkaAuthMode.SASL_PLAINTEXT,
                                  username=kafka_config['user'],
                                  password=kafka_config['password'])

    er.create_namespace(namespace='test', global_context=client_config['global_context'], event_source=kafka)

    # er.delete_namespace('test')