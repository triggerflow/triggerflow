from triggerflow.client import TriggerflowClient, CloudEvent, DefaultActions, DefaultConditions
from triggerflow.cache import get_triggerflow_config
from triggerflow.client.sources import RabbitEventSource


N_MAPS = 20
N_JOIN = 100
TOPIC = 'load_test'


def setup():
    client_config = get_triggerflow_config('~/client_config.yaml')

    tf = TriggerflowClient(**client_config['triggerflow'], caching=True)
    rabbit = RabbitEventSource(**client_config['event_sources']['rabbitmq'])

    tf.create_workspace(workspace=TOPIC, event_source=rabbit)

    for i in range(N_MAPS):
        tf.add_trigger(CloudEvent('map_{}'.format(i)),
                       action=DefaultActions.PASS,
                       condition=DefaultConditions.SIMPLE_JOIN,
                       context={'total_activations': N_JOIN})

    tf.commit_cached_triggers()


if __name__ == '__main__':
    setup()
