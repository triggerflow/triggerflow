import json
from cloudant_client import CloudantClient
from ibm_cloudfunctions_client import CloudFunctionsClient

cd_client = None


def put_trigger(namespace, events, trigger):

    for event in events:
        try:
            triggers = cd_client.get(database_name=namespace,
                                     document_id=event['subject'])
        except KeyError:
            triggers = {'triggers': []}
        triggers['triggers'].append(trigger)
        cd_client.put(database_name=namespace,
                      document_id=event['subject'], data=triggers)

    return {"statusCode": 201, "body": {"message": "created/updated triggers"}}


def check_cloudfunctions_credentials(endpoint, namespace, api_key):
    try:
        cf_client = CloudFunctionsClient(endpoint=endpoint,
                                         namespace=namespace,
                                         api_key=api_key)
        cf_client.list_packages()
        return True
    except Exception:
        return False


def validate_params(params):
    print(params.keys())
    MANDATORY_PARAMS = set(('user_credentials', 'private_credentials'))
    if not MANDATORY_PARAMS.issubset(set(params)):
        raise Exception("Missing mandatory user/private credentials")

    # Check request parameters
    if not any(key in params for key in ['namespace', 'events', 'trigger']):
        raise Exception("Missing mandatory 'trigger' parameter")

    trigger = params['trigger']
    events = params['events']
    namespace = params['namespace']

    # Check Cloud Functions credentials
    user_credentials = params['user_credentials']
    MANDATORY_CF_CREDS = set(('endpoint', 'namespace', 'api_key'))
    if not MANDATORY_CF_CREDS.issubset(set(user_credentials)):
        raise Exception('Invalid user credentials')

    if not check_cloudfunctions_credentials(endpoint=user_credentials['endpoint'],
                                            namespace=user_credentials['namespace'],
                                            api_key=user_credentials['api_key']):
        raise Exception('Invalid IBM Cloud Functions apikey')

    # Check Cloudant credentials
    private_credentials = params['private_credentials']
    if 'cloudant' not in private_credentials:
        raise Exception('Invalid private credentials')

    MANDATORY_CD_CREDS = set(('apikey', 'username'))
    if not MANDATORY_CD_CREDS.issubset(set(private_credentials['cloudant'])):
        raise Exception('Invalid private credentials')

    global cd_client
    try:
        cd_client = CloudantClient(
            username=private_credentials['cloudant']['username'],
            apikey=private_credentials['cloudant']['apikey'])
    except Exception:
        raise Exception('Invalid Cloudant username or apikey')

    return {
        'trigger': trigger,
        'events': events,
        'namespace': namespace
    }


def main(args):
    try:

        try:
            valid_params = validate_params(args)
        except Exception as e:
            raise e
            return {"statusCode": 400, "body": {"error": str(e)}}

        if args['__ow_method'] == 'put':
            res = put_trigger(**valid_params)
        else:
            res = {"statusCode": 405, "body": {
                "error": "Method {} not allowed".format(args['__ow_method'])}}
    except Exception as e:
        raise e
        res = {"statusCode": 500, "body": json.dumps(str(e))}

    return res
