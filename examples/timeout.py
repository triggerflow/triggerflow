from triggerflow import Triggerflow, CloudEvent
from triggerflow.eventsources import RedisEventSource
from triggerflow.functions import PythonCallable

redis_config = {
    'host': '127.0.0.1',
    'port': 6379,
    'db': 0,
    'password': 'myverysecurepassword123'
}

redis_eventsource = RedisEventSource(**redis_config)

tf = Triggerflow()
tf.create_workspace(event_source=redis_eventsource, workspace_name='timeout_test')
# tf.target_workspace('timeout_test')

# Create the timeout event. All timeout events have type 'timeout.triggerflow.event'
timeout_event = CloudEvent().SetEventType('timeout.triggerflow.event').SetSubject('timeout')

def my_action(event, context):
    print('Timeout!')

# Add a trigger that will be activated when the timeout event is received
tf.add_trigger(event=timeout_event,
               trigger_id='my_timeout_trigger',
               action=PythonCallable(my_action))

# Add a timeout. The 'timeout_event' will be published to the 'redis_eventsource' after 10 seconds from now
tf.timeout(timeout_event, redis_eventsource, 10)
