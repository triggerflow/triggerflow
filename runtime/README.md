# Serverless runtimes for Triggerflow

Since current FaaS services don't support termination events (IBM Cloud Functions, Google Cloud Functions...) and those
who support it aren't much flexible or they don't support CloudEvents (AWS Lambda Destinations), we need to adapt
our serverless functions so that they generate a CloudEvent upon termination.
We are currently supporting two runtimes:

## IBM Cloud Functions
For IBM Cloud Functions (based on OpenWhisk) in order to transparently produce a CloudEvent from IBM Cloud Functions,
we have modified the original [container image](ibm_cloud_functions) function handler.
When creating an action, specify triggerflow's IBM Cloud Functions runtime using the `docker` parameter:

```
ibmcloud wsk action create triggerflow-examples/hello-world --docker triggerflow/ibm_cloud_functions_runtime helloworld.py",
```

The function will produce a termination event and send it to the event broker used as trigger's event source.

## AWS Lambda
Add the `triggerflow_handler.py` script provided [here](aws_lambda/triggerflow_handler.py) to your lambda function package.
Then, add the Triggerflow handler decorator to the *entrypoint* of your lambda function:

```python
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
``` 

Your lambda will return a CloudEvent and send it through AWS Lambda Destinatioons if it was invoked from Triggerflow,
else it will behave as a regular lambda.
