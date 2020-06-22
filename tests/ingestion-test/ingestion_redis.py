import redis

from uuid import uuid4
from platform import node
from datetime import datetime

from triggerflow import Triggerflow
from triggerflow.eventsources.redis import RedisEventSource
from triggerflow.functions import DefaultActions, DefaultConditions
from triggerflow.libs.cloudevents.sdk import event


def setup_triggers():
    red = RedisEventSource(host="127.0.0.1", port=6379, password="gPVWL8ttfRffNS7ausKsrtKfQoAIrVglTDWB8TV2")
    tf = Triggerflow()
    tf.create_workspace(workspace_name='ingestion-test', event_source=red)

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
            context={'join': 1000},
            context_parser="JOIN",
            transient=False
        )


def produce_events():
    red = redis.client.StrictRedis(host='127.0.0.1', port=6379,
                                   password="gPVWL8ttfRffNS7ausKsrtKfQoAIrVglTDWB8TV2", db=0)

    for _ in range(1000):
        for i in range(200):
            uuid = uuid4()
            event = {'specversion': '1.0',
                     'id': uuid.hex,
                     'source': f'urn:{node()}:{str(uuid)}',
                     'type': 'event.triggerflow.test',
                     'time': str(datetime.utcnow().isoformat("T") + "Z"),
                     'subject': 'join{}'.format(i)}
            red.xadd('map-a409d7449f13', event)


if __name__ == '__main__':
    setup_triggers()
