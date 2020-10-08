import json
from triggerflow_handler import triggerflow


# Lambda Handler with triggerflow handler decorator
@triggerflow
def lambda_handler(event, context):
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
