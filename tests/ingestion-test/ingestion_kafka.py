import json
from uuid import uuid4
from platform import node
from datetime import datetime
from confluent_kafka import Producer

from triggerflow import Triggerflow
from triggerflow.eventsources import KafkaEventSource
from triggerflow.functions import DefaultActions, DefaultConditions
from triggerflow.libs.cloudevents.sdk import event


def setup_triggers():
    kaf = KafkaEventSource(broker_list=['127.0.0.2:9092'], topic='ingestion')
    tf = Triggerflow()
    tf.create_workspace(workspace_name='ingestion-test', event_source=kaf)

    for i in range(200):
        uuid = uuid4()
        cloudevent = (event.v1.Event()
                      .SetSubject("join{}".format(i))
                      .SetEventType('event.triggerflow.test')
                      .SetEventID(uuid.hex)
                      .SetSource(f'urn:{node()}:{str(uuid)}'))

        tf.add_trigger(
            event=cloudevent,
            trigger_id="join{}".format(i),
            condition=DefaultConditions.SIMPLE_JOIN,
            action=DefaultActions.TERMINATE,
            context={'join': 5000},
            context_parser="JOIN",
            transient=False
        )


def produce_events():
    config = {'bootstrap.servers': '127.0.0.2:9092'}
    kafka_producer = Producer(**config)

    def delivery_callback(err, msg):
        if err:
            print('Failed delivery: {}'.format(err))
        else:
            print('Message delivered: {} {} {}'.format(msg.topic(), msg.partition(), msg.offset()))
    for _ in range(5000):
        for i in range(200):
            uuid = uuid4()
            event = {'specversion': '1.0',
                     'id': uuid.hex,
                     'source': f'urn:{node()}:{str(uuid)}',
                     'type': 'event.triggerflow.test',
                     'time': str(datetime.utcnow().isoformat("T") + "Z"),
                     'subject': 'join{}'.format(i)}
            kafka_producer.produce(topic='ingestion',
                                   value=json.dumps(event),
                                   callback=delivery_callback)
        kafka_producer.flush()


if __name__ == '__main__':
    # setup_triggers()
    produce_events()
