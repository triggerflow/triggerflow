import random


def lambda_handler(event, context):
    size = int(event['size'])
    l = [random.randint(0, 100) for _ in range(size)]
    subject = event.get('__TRIGGERFLOW_SUBJECT', None)
    return {'list': l, '__TRIGGERFLOW_SUBJECT': subject}
