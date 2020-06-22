from triggerflow import Triggerflow
from triggerflow.functions import PythonCallable
from triggerflow.eventsources.rabbit import RabbitMQEventSource
from triggerflow.libs.cloudevents.sdk.event.v1 import Event

tf_client = Triggerflow()

rabbitmq_source = RabbitMQEventSource(amqp_url='amqp://guest:guest@172.17.0.3/', queue='My-Queue')

tf_client.create_workspace(workspace_name='test', event_source=rabbitmq_source)


def my_action(context, event):
    context['message'] += 'World!'


activation_event = Event().SetEventType('test.event.type').SetSubject('Test')

tf_client.add_trigger(trigger_id='MyTrigger',
                      event=activation_event,
                      action=PythonCallable(my_action),
                      context={'message': 'Hello '})

rabbitmq_source.publish_cloudevent(activation_event)

trg = tf_client.get_trigger('MyTrigger')
print(trg['context']['message'])
