import random


def lambda_handler(event, context):
    while True:
        n = random.randint(-5, 5)
        if n != 0:
            break
    subject = event.get('__TRIGGERFLOW_SUBJECT', None)
    return {'random': n, '__TRIGGERFLOW_SUBJECT': subject}
