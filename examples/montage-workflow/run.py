import json

from triggerflow.statemachine import StateMachine
from triggerflow.eventsources import KafkaEventSource

with open('montage.json', 'r') as sm_file:
    sm = json.loads(sm_file.read())

kafka_source = KafkaEventSource(broker_list=['192.168.0.1:9092'])

sm = StateMachine.json('montage.json', event_source=kafka_source)
sm.trigger()
