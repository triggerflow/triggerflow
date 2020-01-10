from uuid import uuid4
from utils import auth_request, parse_path


def add_trigger(db, path, params):
    ok, res = auth_request(db, params)
    if not ok:
        return res

    path = parse_path(path)
    if not db.exists(database_name=path.namespace):
        return {"statusCode": 409, "body": {"error": "Namespace {} does not exists".format(path.namespace)}}

    namespace = path.namespace
    trigger = params['trigger']
    events = params['events']

    source_events = db.get(database_name=namespace, document_id='.events')
    triggers = db.get(database_name=namespace, document_id='.triggers')

    # Add trigger to database
    trigger_id = trigger['id'] if not trigger['transient'] else str(uuid4())
    triggers[trigger_id] = trigger

    # Link source events to the trigger added
    for event in events:
        if event['subject'] in source_events:
            source_events[event['subject']].append(trigger_id)
        else:
            source_events[event['subject']] = [trigger_id]

    db.put(database_name=namespace, document_id='.events', data=source_events)
    db.put(database_name=namespace, document_id='.triggers', data=triggers)

    return {"statusCode": 201, "body": {"trigger_id": trigger_id}}


def get_trigger(db, path, params):
    pass


def delete_trigger(db, path, params):
    pass
