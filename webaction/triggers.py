from uuid import uuid4
import utils


def init(db, params):
    token = utils.gen_token()

    namespace = params['namespace']

    if 'global_context' in params:
        global_context = params['global_context']
        db.put(database_name=namespace, document_id='global_context', data=global_context)
    else:
        global_context = {}

    if 'event_source' in params:
        event_source = params['event_source']
        db.put(database_name=namespace, document_id='event_source', data=event_source)
    else:
        try:
            event_source = db.get(database_name=namespace, document_id='event_source')
        except KeyError:
            return {'statusCode': 400, 'body': {'error': 'Event source not found for namespace {}'.format(namespace)}}

    # TODO send request to init worker

    return {'statusCode': 200, 'body': {'token': token,
                                        'namespace': namespace,
                                        'global_context': global_context,
                                        'event_source': event_source}}


def add_trigger(db, path, params):
    namespace = params['namespace']

    # Authenticate request
    ok, res = utils.auth_request(params)
    if not ok:
        return res

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

    return {"statusCode": 201, "body": {"trigger_id": trigger_id}}


def get_trigger(db, path, params):
    pass


def delete_trigger(db, path, params):
    pass
