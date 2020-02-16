from uuid import uuid4
from utils import parse_path


def add_trigger(db, path, params):
    path = parse_path(path)
    if not db.database_exists(database_name=path.namespace):
        return {"statusCode": 409, "body": {"error": "Namespace {} does not exists".format(path.namespace)}}

    namespace = path.namespace
    triggers = params['triggers']
    committed_triggers = []
    failed_trigger_commit = {}

    if not triggers:
        return {"statusCode": 400, "body": {"error": "Trigger list is empty"}}
    elif len(triggers) == 1:  # Commit a single trigger

        trigger = triggers.pop()

        if trigger['trigger_id']:  # Named trigger, check if it already exists
            if db.key_exists(database_name=path.namespace, document_id='.triggers', key=trigger['trigger_id']):
                return {"statusCode": 409, "body": {"error": "Trigger {} already exists".format(trigger['trigger_id'])}}
        elif not trigger['trigger_id'] and trigger['transient']:  # Unnamed trigger, check if it is transient
            trigger['trigger_id'] = str(uuid4())
        else:  # Unnamed non-transient trigger: illegal
            return {"statusCode": 400, "body": {"error": "Non-transient unnamed trigger".format(trigger['trigger_id'])}}

        db.set_key(database_name=namespace, document_id='.triggers', key=trigger['trigger_id'], value=trigger)
        committed_triggers.append(trigger['trigger_id'])
    else:  # Commit multiple triggers

        db_triggers = db.get(database_name=path.namespace, document_id='.triggers')

        for i, trigger in enumerate(triggers):
            if trigger['trigger_id']:  # Named trigger, check if it already exists
                if trigger['trigger_id'] in db_triggers:
                    failed_trigger_commit[i] = 'Trigger {} already exists'.format(trigger['trigger_id'])
            elif not trigger['trigger_id'] and trigger['transient']:  # Unnamed trigger, check if it is transient
                trigger['trigger_id'] = str(uuid4())
            else:  # Unnamed non-transient trigger: illegal
                failed_trigger_commit[i] = 'Non-transient unnamed trigger'
            db_triggers[trigger['trigger_id']] = trigger

        db.put(database_name=namespace, document_id='.triggers', data=db_triggers)

    return {"statusCode": 201, "body": {"triggers": committed_triggers}}


def get_trigger(db, path, params):
    pass


def delete_trigger(db, path, params):
    pass
