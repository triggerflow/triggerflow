from eventprocessor_client import CloudEventProcessorClient, CloudEvent, DockerImage, DefaultActions
from eventprocessor_client.utils import load_config_yaml
from eventprocessor_client.sources.kafka import KafkaCloudEventSource

if __name__ == "__main__":
    client_config = load_config_yaml('~/client_config.yaml')
    kafka_config = client_config['event_sources']['kafka']

    er = CloudEventProcessorClient(**client_config['event_processor'])

    kafka = KafkaCloudEventSource(name='my_kafka_eventsource',
                                  broker_list=kafka_config['broker_list'],
                                  topic='my_topic')

    er.create_namespace(namespace='docker_condition_example',
                        global_context=client_config['global_context'],
                        event_source=kafka)

    url = 'https://hello'

    er.add_trigger(CloudEvent('hello'),
                   condition=DockerImage(image='aitorarjona/threshold_condition:latest',
                                         class_name='my_condition.counter_threshold'),
                   action=DefaultActions.IBM_CF_INVOKE_KAFKA,
                   context={'threshold': 5,
                            'subject': 'ca1',
                            'function_args': {},
                            'function_url': url,
                            'kind': 'callasync'})
