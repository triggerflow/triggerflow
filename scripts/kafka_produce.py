import json
import sys
from uuid import uuid4
from platform import node
from confluent_kafka import Producer
from triggerflow import CloudEvent

TOPIC = 'map-a706f6ade13d'

if __name__ == '__main__':
    # assert len(sys.argv) == 4, 'Usage: {} <subject> <type> <data>'.format(sys.argv[0])

    # subject = sys.argv[1]
    # type = sys.argv[2]
    # data = json.loads(sys.argv[3])
    subject = '__init__'
    type = 'event.triggerflow.init'
    data = {}

    config = {'bootstrap.servers': ':9092'}


    def delivery_callback(err, msg):
        if err:
            print('Failed delivery: {}'.format(err))
        else:
            print('Message delivered: {} {} {}'.format(msg.topic(), msg.partition(), msg.offset()))


    kafka_producer = Producer(**config)
    uuid = uuid4()
    cloudevent = (CloudEvent()
                  .SetSubject(subject)
                  .SetEventType(type)
                  .SetEventID(uuid.hex)
                  .SetSource(f'urn:{node()}:{str(uuid)}'))
    if data:
        print(data)
        cloudevent.SetContentType('application/json')
        cloudevent.SetData(data)
    payload = cloudevent.MarshalJSON(json.dumps).read().decode('utf-8')
    print(payload)
    kafka_producer.produce(topic=TOPIC,
                           value=payload,
                           callback=delivery_callback)
    kafka_producer.flush()
