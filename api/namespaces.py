import re
import requests
from utils import parse_path


def add_namespace(db, path, params):
    path = parse_path(path)
    if db.exists(database_name=path.namespace):
        return {"statusCode": 409, "body": {"error": "Namespace {} already exists".format(path.namespace)}}
    elif not re.fullmatch(r"^[a-zA-Z0-9_.-]*$", path.namespace):
        return {"statusCode": 400, "body": {"error": "Illegal namespace name".format(path.namespace)}}

    if 'event_source' in params and params['event_source'] is not None:
        event_sources = {params['event_source']['name']: params['event_source'].copy()}
    else:
        event_sources = {}

    db.put(database_name=path.namespace, document_id='.event_sources', data=event_sources)
    db.put(database_name=path.namespace, document_id='.trigger_events', data={})
    db.put(database_name=path.namespace, document_id='.triggers', data={})
    db.put(database_name=path.namespace, document_id='.global_context', data=params['global_context'])

    requests.post(params['private_credentials']['event-router-endpoint'],
                  json={'namespace': path.namespace})

    return {"statusCode": 201, "body": {"created": path.namespace}}


def get_namespace(db, path, params):
    pass


def delete_namespace(db, path, params):
    pass
