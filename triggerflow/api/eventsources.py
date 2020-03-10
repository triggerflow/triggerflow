from .utils import parse_path


def add_eventsource(db, path, params):
    path = parse_path(path)
    if not db.workspace_exists(workspace=path.workspace):
        return {"statusCode": 404, "body": {"error": "Workspace {} does not exists".format(path.workspace)}}

    event_sources = db.get(workspace=path.workspace, document_id='event_sources')

    if path.eventsource not in event_sources:
        event_sources[path.eventsource] = params['eventsource'].copy()
        res = {"statusCode": 201, "body": {"created": path.eventsource}}
        db.put(workspace=path.workspace, document_id='event_sources', data=event_sources)
    elif path.eventsource in event_sources and params['overwrite']:
        event_sources[path.eventsource] = params['eventsource'].copy()
        res = {"statusCode": 201, "body": {"updated": path.eventsource}}
        db.put(workspace=path.workspace, document_id='event_sources', data=event_sources)
    else:
        res = {"statusCode": 409, "body": {"error": "{} already exists".format(path.eventsource)}}

    return res


def list_eventsources(db, path, params):
    path = parse_path(path)
    if not db.workspace_exists(workspace=path.workspace):
        return {"statusCode": 404, "body": {"error": "Workspace {} does not exists".format(path.workspace)}}

    event_sources = db.get(workspace=path.workspace, document_id='event_sources')

    return {"statusCode": 200, "body": {"event_sources": list(event_sources.keys())}}


def get_eventsource(db, path, params):
    path = parse_path(path)
    if not db.workspace_exists(workspace=path.workspace):
        return {"statusCode": 404, "body": {"error": "Workspace {} does not exists".format(path.workspace)}}

    event_source = db.get_key(workspace=path.workspace, document_id='event_sources', key=path.eventsource)

    if event_source is None:
        return {"statusCode": 404, "body": {"error": "Eventsource {} does not exist".format(path.eventsource)}}
    else:
        return {"statusCode": 200, "body": {path.eventsource: event_source}}


def delete_eventsource(db, path, params):
    path = parse_path(path)
    if not db.workspace_exists(workspace=path.workspace):
        return {"statusCode": 404, "body": {"error": "Workspace {} does not exists".format(path.workspace)}}

    db.set_key(workspace=path.workspace, document_id='event_sources', key=path.eventsource, value=None)

    return {"statusCode": 200, "body": {"deleted": path.eventsource}}
