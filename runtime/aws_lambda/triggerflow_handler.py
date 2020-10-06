import json
from datetime import datetime


# Triggerflow Handler
def triggerflow(handler):
    def decorator(*args, **kwargs):
        event, context = args[0], args[1]
        try:
            subject = event['__TRIGGERFLOW_SUBJECT']
            is_triggerflow = subject is not None and '__EVENT_DATA' in event
        except:
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

            return cloudevent
        else:
            return result
    return decorator


# Lambda Handler with triggerflow handler decorator
@triggerflow
def lambda_handler(event, context):
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
