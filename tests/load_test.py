from triggerflow.client import TriggerflowCachedClient, CloudEvent, DefaultActions, DefaultConditions
from triggerflow.eventsources.rabbit import RabbitMQEventSource


N_MAPS = 20
N_JOIN = 100
TOPIC = 'load_test'


def setup():
    amqp_url = ''
    tf = TriggerflowCachedClient()
    rabbit = RabbitMQEventSource(amqp_url)

    tf.create_workspace(workspace_name=TOPIC, event_source=rabbit)

    for i in range(N_MAPS):
        ce = CloudEvent().SetEventType('triggerflow.event.success').SetSubject('map_{}'.format(i))
        tf.add_trigger(ce,
                       action=DefaultActions.PASS,
                       condition=DefaultConditions.SIMPLE_JOIN,
                       context={'total_activations': N_JOIN})

    tf.commit_cached_triggers()


if __name__ == '__main__':
    setup()
