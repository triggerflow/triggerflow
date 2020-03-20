import re
import requests
from requests.auth import HTTPBasicAuth
from .utils import parse_path, get_authentication_parameters


def add_workspace(db, path, params):
    path = parse_path(path)

    if not db.workspace_exists(workspace=path.workspace):
        if not re.fullmatch(r"^[a-zA-Z0-9.-]*$", path.workspace):
            return {"statusCode": 400, "body": {"error": "Illegal workspace name".format(path.workspace)}}

        if 'event_source' in params:
            name = params['event_source']['name']
            event_sources = {name: params['event_source']}
        else:
            event_sources = {}

        if 'global_context' in params:
            global_context = params['global_context']
        else:
            global_context = {}
        global_context['event_sources'] = event_sources

        db.create_workspace(path.workspace, event_sources, global_context)

    user, password = get_authentication_parameters(params)
    auth = HTTPBasicAuth(username=user, password=password)

    try:
        res = requests.post('/'.join([params['credentials']['triggerflow_service']['endpoint'],
                                      'workspace', path.workspace]), auth=auth, json={})
        if res.ok:
            return {"statusCode": 201, "body": {"created": path.workspace}}
        else:
            return {"statusCode": res.status_code, "body": {"error": res.text}}

    except requests.exceptions.HTTPError:
        return {"statusCode": 503, "body": {"error": "Triggerflow service unavailable"}}


def get_workspace(db, path, params):
    pass


def delete_workspace(db, path, params):
    path = parse_path(path)
    if not db.workspace_exists(workspace=path.workspace):
        return {"statusCode": 404, "body": {"error": "Workspace {} not found".format(path.workspace)}}
    else:
        db.delete(workspace=path.workspace)

    user, password = get_authentication_parameters(params)
    auth = HTTPBasicAuth(username=user, password=password)

    try:
        res = requests.delete('/'.join([params['credentials']['triggerflow_service']['endpoint'],
                                        'workspace', path.workspace]), auth=auth, json={})
        if res.ok:
            return {"statusCode": 201, "body": {"deleted": path.workspace}}
        else:
            return {"statusCode": res.status_code, "body": {"error": res.text}}
    except requests.exceptions.HTTPError:
        return {"statusCode": 503, "body": {"error": "Triggerflow service unavailable"}}
