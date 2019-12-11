from api.client import CloudEventProcessorClient, DefaultActions, DefaultConditions
from api.utils import load_config_yaml
from api.sources.rabbit import RabbitMQCloudEventSource

if __name__ == "__main__":
    client_config = load_config_yaml('~/event-processor_credentials.yaml')
    rabbit_credentials = load_config_yaml('~/rabbit_credentials.yaml')

    rabbit = RabbitMQCloudEventSource(amqp_url=rabbit_credentials['rabbitmq']['amqp_url'],
                                      topic='hello')

    er = CloudEventProcessorClient(namespace='ibm_cf_test_rabbit',
                                   event_source=rabbit,
                                   global_context={
                                       'ibm_cf_credentials': client_config['authentication']['ibm_cf_credentials']},
                                   api_endpoint=client_config['event_processor']['api_endpoint'],
                                   authentication=client_config['authentication'])

    # init__ >> ca1 >> [map1, ca2] >> map2 >> ca3 >> end__

    url = 'https://us-east.functions.cloud.ibm.com/api/v1/namespaces/cloudlab_urv_us_east/actions/eventprocessor_functions/rabbit_test'
    er.add_trigger(rabbit.event('init__'),
                   action=DefaultActions.IBM_CF_INVOKE_RABBITMQ,
                   context={'subject': 'ca1', 'url': url, 'args': {'iter': 1}, 'kind': 'callasync'})

    er.add_trigger(rabbit.event('ca1'),
                   condition=DefaultConditions.IBM_CF_JOIN,
                   action=DefaultActions.IBM_CF_INVOKE_RABBITMQ,
                   context={'subject': 'map1',
                            'url': url,
                            'args': [{'iter': 1}, {'iter': 2}, {'iter': 3}],
                            'kind': 'map'})
    er.add_trigger(rabbit.event('ca1'),
                   condition=DefaultConditions.IBM_CF_JOIN,
                   action=DefaultActions.IBM_CF_INVOKE_RABBITMQ,
                   context={'subject': 'ca2', 'url': url, 'args': {'iter': 1}, 'kind': 'callasync'})

    er.add_trigger([rabbit.event('map1'), rabbit.event('ca2')],
                   condition=DefaultConditions.IBM_CF_JOIN,
                   action=DefaultActions.IBM_CF_INVOKE_RABBITMQ,
                   context={'subject': 'map2', 'url': url, 'args': [{'iter': 1}, {'iter': 2}], 'kind': 'map'})

    er.add_trigger(rabbit.event('map2'),
                   condition=DefaultConditions.IBM_CF_JOIN,
                   action=DefaultActions.IBM_CF_INVOKE_RABBITMQ,
                   context={'subject': 'ca3', 'url': url, 'args': {'iter': 1}, 'kind': 'callasync'})

    er.add_trigger(rabbit.event('ca3'),
                   condition=DefaultConditions.IBM_CF_JOIN,
                   action=DefaultActions.TERMINATE)
