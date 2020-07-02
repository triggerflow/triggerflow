

def lambda_handler(event, context):
    n = int(event['number'])
    n = n * -1 if n < 0 else n
    subject = event.get('__TRIGGERFLOW_SUBJECT', None)
    return {'number': n, '__TRIGGERFLOW_SUBJECT': subject}
