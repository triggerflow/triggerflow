

def lambda_handler(event, context):
    res = event['x'] + event['y']
    subject = event.get('__TRIGGERFLOW_SUBJECT', None)
    return {'result': res, '__TRIGGERFLOW_SUBJECT': subject}
