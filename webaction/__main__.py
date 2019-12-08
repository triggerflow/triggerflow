import dateutil.parser
from datetime import datetime
from uuid import uuid4

import utils
from cloudant_client import CloudantClient

db = None


def add_trigger(params):
    global db
    namespace = params['namespace']

    # Authenticate request
    if 'token' not in params['user_credentials']:
        return {'statusCode': 401, 'body': {'error': "Missing 'token' parameter"}}
    token = params['user_credentials']['token']
    sessions = db.get(database_name=namespace, document_id='sessions')
    if token in sessions:
        emitted_token_time = dateutil.parser.parse(sessions[token])
        seconds = (datetime.utcnow() - emitted_token_time).seconds
        if seconds > 3600:
            return {'statusCode': 403, 'body': {'error': 'Token expired'}}
    else:
        return {'statusCode': 401, 'body': {'error': 'Unauthorized'}}

    # Check if trigger and events keys are in params
    if not {'trigger', 'events'}.issubset(params):
        return {'statusCode': 400, 'body': {'error': "Missing 'trigger' and/or 'events' parameters"}}

    # Get triggers and source events from remote db
    try:
        source_events = db.get(database_name=namespace, document_id='source_events')
    except KeyError:
        source_events = {}
    try:
        triggers = db.get(database_name=namespace, document_id='triggers')
    except KeyError:
        triggers = {}

    # Add trigger to database
    trigger_id = str(uuid4())
    triggers[trigger_id] = params['trigger']

    # Link source events to the trigger added
    events = params['events']
    for event in events:
        if event['subject'] in source_events:
            source_events[event['subject']].append(trigger_id)
        else:
            source_events[event['subject']] = [trigger_id]

    db.put(database_name=namespace, document_id='source_events', data=source_events)
    db.put(database_name=namespace, document_id='triggers', data=triggers)

    return {"statusCode": 201, "body": {"message": "created/updated triggers"}}


def init(params):
    namespace = params['namespace']
    global db
    # Check if ibm credentials are valid
    try:
        ibm_cf_credentials = params['user_credentials']['ibm_cf_credentials']
        ok = utils.check_cloudfunctions_credentials(**ibm_cf_credentials)
        if not ok:
            raise Exception('Invalid IBM Cloud Functions credentials')
    except KeyError:
        return {'statusCode': 400, 'body': {'error': 'Missing parameters'}}
    except Exception as e:
        return {'statusCode': 400, 'body': {'error': str(e)}}

    # Generate session token
    token = utils.generate_token()
    timestamp = str(datetime.utcnow().isoformat())

    try:
        sessions = db.get(database_name=namespace, document_id='sessions')
    except KeyError:
        sessions = {}
    sessions[token] = timestamp
    db.put(database_name=namespace, document_id='sessions', data=sessions)

    if 'default_context' in params:
        db.put(database_name=namespace, document_id='default_context',
               data={'default_context': params['default_context']})

    # TODO send request to init worker

    return {'statusCode': 200, 'body': {'token': token}}


def main(args):
    print(args)
    params = utils.validate_params(args)
    global db
    db = CloudantClient(username=params['private_credentials']['cloudant']['username'],
                        apikey=params['private_credentials']['cloudant']['apikey'])
    res = {"statusCode": 404, "body": {"error": "Not found"}}
    try:
        if args['__ow_path'] == '/init':
            if args['__ow_method'] == 'put':
                res = init(params)
        elif args['__ow_path'] == '/add_trigger':
            if args['__ow_method'] == 'put':
                res = add_trigger(params)
        return res
    except Exception as e:
        raise e
        return {"statusCode": 500, "body": "Internal error: {}".format(str(e))}
