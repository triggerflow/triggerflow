import time
from triggerflow import Triggerflow, CloudEvent
from triggerflow.functions import PythonCallable, python_object
from triggerflow.eventsources.rabbit import RabbitMQEventSource

tf_client = Triggerflow(endpoint='127.0.0.1:8080', user='admin', password='admin')
tf_client.target_workspace('python_object_test')

rabbitmq_source = RabbitMQEventSource(amqp_url='amqp://guest:guest@192.168.1.43/', queue='python_object_test')

tf_client.create_workspace(workspace_name='python_object_test', event_source=rabbitmq_source)


class MyClass:
    message = 'Hello'


def my_action(context, event):
    x = context['my_class']
    context['instance'] = python_object(x())

activation_event = CloudEvent().SetEventType('test.event.type').SetSubject('Test')

tf_client.add_trigger(trigger_id='MyTrigger',
                      event=activation_event,
                      action=PythonCallable(my_action),
                      context={'my_class': python_object(MyClass)})

rabbitmq_source.publish_cloudevent(activation_event)

time.sleep(1.5)  # Let some time for the DB to be updated

trg = tf_client.get_trigger('MyTrigger')
print(trg['context']['instance'])
