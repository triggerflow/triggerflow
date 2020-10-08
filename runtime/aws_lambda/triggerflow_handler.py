import json
from uuid import uuid1
from confluent_kafka import Producer
from datetime import datetime


def triggerflow(handler):
    def decorator(*args, **kwargs):
        event, context = args[0], args[1]
        if '__TRIGGERFLOW_SUBJECT' in event:
            subject = event['__TRIGGERFLOW_SUBJECT']
            is_triggerflow = subject is not None and '__EVENT_DATA' in event
        else:
            event_data = event
            is_triggerflow = False

        if is_triggerflow:
            try:
                event_data = json.loads(event['__EVENT_DATA'])
            except:
                event_data = event['__EVENT_DATA']

        result = handler(event_data, context)

        if is_triggerflow:
            cloudevent = {
                'specversion': '1.0',
                'id': context.aws_request_id,
                'source': context.invoked_function_arn,
                'type': 'lambda.success',
                'time': str(datetime.utcnow().isoformat("T") + "Z"),
                'subject': subject
            }

            if result is not None:
                if isinstance(result, dict):
                    cloudevent['datacontenttype'] = 'application/json'
                    cloudevent['data'] = result
                elif isinstance(result, str):
                    cloudevent['datacontenttype'] = 'plain/text'
                    cloudevent['data'] = result

            if '__TRIGGERFLOW_EVENT_SOURCE' in event:
                event_source = event['__TRIGGERFLOW_EVENT_SOURCE']
                if event_source['class'] == 'KafkaEventSource':
                    params = event_source['parameters']
                    config = {'bootstrap.servers': ','.join(params['broker_list']),
                              'group.id': str(uuid1())}
                    kafka_producer = Producer(**config)
                    kafka_producer.produce(topic=params['topic'],
                                           value=json.dumps(cloudevent))
                    kafka_producer.flush()

            return cloudevent
        else:
            return result
    return decorator


