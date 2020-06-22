import logging
from pprint import pprint
from triggerflow import Triggerflow
from triggerflow.libs.cloudevents.sdk.event import v1
from triggerflow.eventsources import KafkaEventSource, RabbitMQEventSource

logging.basicConfig()
logging.root.setLevel(logging.DEBUG)

tf = Triggerflow()
tf.target_workspace('test')

# Workspace
kafka = KafkaEventSource(topic='KafkaTopicTest', broker_list=['192.168.0.1:9092'])

tf.create_workspace(workspace_name='test', event_source=kafka)

# Event sources
rabbit = RabbitMQEventSource(name='MyRabbitMQEventSource', amqp_url='amqp://guest:guest@localhost', queue='event_queue')

tf.add_event_source(event_source=rabbit)

event_source = tf.get_event_source(event_source_name='MyRabbitMQEventSource')
pprint(event_source)

tf.delete_event_source(event_source_name='MyRabbitMQEventSource')

event_sources = tf.list_event_sources()
pprint(event_sources)

# Triggers
event = (v1.Event()
         .SetSubject('Hello')
         .SetEventType('test.trigger.client'))
tf.add_trigger(event=event,
               trigger_id='MyTrigger',
               context={'Hello': 'Triggerflow'})

trigger = tf.get_trigger(trigger_id='MyTrigger')
pprint(trigger)

event = (v1.Event()
         .SetSubject('Bye')
         .SetEventType('test.trigger.client'))
tf.add_trigger(event=event, trigger_id='MySecondTrigger')

triggers = tf.list_triggers()
pprint(triggers)

tf.delete_trigger('MySecondTrigger')

triggers = tf.list_triggers()
pprint(triggers)

tf.flush_triggers()

# Clean
tf.delete_workspace()
