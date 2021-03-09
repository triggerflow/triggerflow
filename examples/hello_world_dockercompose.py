import time
from triggerflow import Triggerflow, CloudEvent
from triggerflow.functions import PythonCallable
from triggerflow.eventsources.rabbit import RabbitMQEventSource

# This example is intended to work with docker-compose deployment

# We can access the API through port 8080 using the address where docker-compose is running
# in this case, we will run this script and docker-compose in the same machine so we can use localhost
tf_client = Triggerflow(endpoint='127.0.0.1:8080', user='admin', password='admin')

# However, if we use localhost for the RabbitMQ ampq URL, the tirgger service will not be accessible to
# connect to the RabbitMQ broker, since 127.0.0.1 for the trigger service container will be itself
# We have to change it with the hostname of the RabbitMQ container. In docker-compose, we can use the
# container name as the hostname and the trigger service will be able to resolve it.
rabbitmq_source = RabbitMQEventSource(amqp_url='amqp://guest:guest@rabbit:5672/', queue='My-Queue')

tf_client.create_workspace(workspace_name='test', event_source=rabbitmq_source)


def my_action(context, event):
    context['message'] += 'World!'


activation_event = CloudEvent().SetEventType('test.event.type').SetSubject('Test')

tf_client.add_trigger(trigger_id='MyTrigger',
                      event=activation_event,
                      action=PythonCallable(my_action),
                      context={'message': 'Hello '})

# Here we have to change again the RabbitMQ ampq URL. We put 'rabbit' before so the trigger service container
# can resolve it correctly, but from our local machine we can't resolve 'rabbit' to localhost unless we change
# /etc/hosts or whatever, so we have to change it to localhost for our machine to access the RabbitMQ container
# running on docker-compose
rabbitmq_source.amqp_url = 'amqp://guest:guest@127.0.0.1:5672/'

rabbitmq_source.publish_cloudevent(activation_event)

time.sleep(3)  # Let some time for the DB to be updated

trg = tf_client.get_trigger('MyTrigger')
print(trg['context']['message'])
