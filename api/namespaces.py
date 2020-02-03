import re
import requests
from requests.auth import HTTPBasicAuth
from utils import parse_path, get_authentication_parameters


def add_namespace(db, path, params):
    path = parse_path(path)

    if not db.database_exists(database_name=path.namespace):
        if not re.fullmatch(r"^[a-zA-Z0-9_.-]*$", path.namespace):
            return {"statusCode": 400, "body": {"error": "Illegal namespace name".format(path.namespace)}}

        if 'event_source' in params and params['event_source'] is not None:
            event_sources = {params['event_source']['name']: params['event_source'].copy()}
        else:
            event_sources = {}

        db.put(database_name=path.namespace, document_id='.event_sources', data=event_sources)
        db.put(database_name=path.namespace, document_id='.triggers', data={})
        db.put(database_name=path.namespace, document_id='.events', data={})
        db.put(database_name=path.namespace, document_id='.global_context', data=params['global_context'])

    user, password = get_authentication_parameters(params)
    auth = HTTPBasicAuth(username=user, password=password)

    try:
        res = requests.post('/'.join([params['private_credentials']['event-router-endpoint'],
                            'namespace', path.namespace]), auth=auth, json={})
        if res.ok:
            return {"statusCode": 201, "body": {"created": path.namespace}}
        else:
            return {"statusCode": res.status_code, "body": {"error": res.text}}

    except requests.exceptions.HTTPError:
        return {"statusCode": 503, "body": {"error": "event processor service unavailable"}}



def get_namespace(db, path, params):
    pass


def delete_namespace(db, path, params):
    path = parse_path(path)
    if not db.database_exists(database_name=path.namespace):
        return {"statusCode": 404, "body": {"error": "Namespace {} not found".format(path.namespace)}}
    else:
        db.delete(database_name=path.namespace)

    user, password = get_authentication_parameters(params)
    auth = HTTPBasicAuth(username=user, password=password)

    try:
        res = requests.delete('/'.join([params['private_credentials']['event-router-endpoint'],
                              'namespace', path.namespace]), auth=auth, json={})
        if res.ok:
            return {"statusCode": 201, "body": {"deleted": path.namespace}}
        else:
            return {"statusCode": res.status_code, "body": {"error": res.text}}
    except requests.exceptions.HTTPError:
        return {"statusCode": 503, "body": {"error": "event processor service unavailable"}}
