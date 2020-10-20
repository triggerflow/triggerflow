from uuid import uuid4
from platform import node

from triggerflow import Triggerflow, CloudEvent, DefaultActions, DefaultConditions
from triggerflow.eventsources import RedisEventSource

stream = 'ingestion'


def setup_triggers():
    red = RedisEventSource(host="127.0.0.1", port=6379, password="potato")
    red.set_stream(stream)
    tf = Triggerflow()
    tf.create_workspace(workspace_name='ingestion-test', event_source=red)

    for i in range(200):
        uuid = uuid4()
        cloudevent = (CloudEvent()
                      .SetSubject("join{}".format(i))
                      .SetEventType('event.triggerflow.test')
                      .SetEventID(uuid.hex)
                      .SetSource(f'urn:{node()}:{str(uuid)}'))

        tf.add_trigger(
            event=cloudevent,
            trigger_id="join{}".format(i),
            condition=DefaultConditions.JOIN,
            action=DefaultActions.TERMINATE,
            context={'join': 1000},
            transient=False
        )


if __name__ == '__main__':
    setup_triggers()
