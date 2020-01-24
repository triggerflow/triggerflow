from uuid import uuid4
from utils import parse_path


def add_trigger(db, path, params):
    path = parse_path(path)
    if not db.database_exists(database_name=path.namespace):
        return {"statusCode": 409, "body": {"error": "Namespace {} does not exists".format(path.namespace)}}

    namespace = path.namespace
    trigger = params['trigger']

    if trigger['id'] is not None:
        if db.key_exists(database_name=path.namespace, document_id='.triggers', key=trigger['id']):
            return {"statusCode": 409, "body": {"error": "Trigger {} already exists".format(trigger['id'])}}


    # Add trigger to database
    trigger['id'] = trigger['id'] if not trigger['transient'] else str(uuid4())

    db.set_key(database_name=namespace, document_id='.triggers', key=trigger['id'], value=trigger)

    return {"statusCode": 201, "body": {"trigger_id": trigger['id']}}


def get_trigger(db, path, params):
    pass


def delete_trigger(db, path, params):
    pass
