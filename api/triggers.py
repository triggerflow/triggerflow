from uuid import uuid4
from utils import parse_path


def add_trigger(db, path, params):
    path = parse_path(path)
    if not db.database_exists(database_name=path.namespace):
        return {"statusCode": 409, "body": {"error": "Namespace {} does not exists".format(path.namespace)}}

    namespace = path.namespace
    triggers = params['triggers']
    committed_triggers = []

    for trigger in triggers:
        # Check if trigger already exists
        if trigger['trigger_id'] is not None:
            if db.key_exists(database_name=path.namespace, document_id='.triggers', key=trigger['trigger_id']):
                return {"statusCode": 409, "body": {"error": "Trigger {} already exists".format(trigger['trigger_id'])}}

        # Add trigger to database
        trigger['trigger_id'] = trigger['trigger_id'] if not trigger['transient'] else str(uuid4())

        db.set_key(database_name=namespace, document_id='.triggers', key=trigger['trigger_id'], value=trigger)
        committed_triggers.append(trigger['trigger_id'])

    return {"statusCode": 201, "body": {"triggers": committed_triggers}}


def get_trigger(db, path, params):
    pass


def delete_trigger(db, path, params):
    pass
