from eventprocessor_client import CloudEventProcessorClient, CloudEvent, DefaultActions
from eventprocessor_client.utils import load_config_yaml
from eventprocessor_client.sources.interfaces.kafka import KafkaCloudEventSource

if __name__ == "__main__":
    client_config = load_config_yaml('~/client_config.yaml')
    kafka_config = client_config['event_sources']['kafka']

    er = CloudEventProcessorClient(**client_config['event_processor'])

    kafka = KafkaCloudEventSource(name='stress_kafka',
                                  broker_list=kafka_config['broker_list'],
                                  topic='stress_kafka')

    er.create_namespace(namespace='stress_kafka', event_source=kafka)