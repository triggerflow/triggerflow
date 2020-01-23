from utils import parse_path


def add_eventsource(db, path, params):
    path = parse_path(path)
    try:
        event_sources = db.get(database_name=path.namespace, document_id='.event_sources')
    except KeyError:
        return {"statusCode": 404, "body": {"error": "Namespace {} not found".format(path.eventsource)}}

    if path.eventsource not in event_sources:
        event_sources[path.eventsource] = params['eventsource'].copy()
        res = {"statusCode": 201, "body": {"created": path.eventsource}}
        db.put(database_name=path.namespace, document_id='.event_sources', data=event_sources)
    elif path.eventsource in event_sources and params['overwrite']:
        event_sources[path.eventsource] = params['eventsource'].copy()
        res = {"statusCode": 201, "body": {"updated": path.eventsource}}
        db.put(database_name=path.namespace, document_id='.event_sources', data=event_sources)
    else:
        res = {"statusCode": 409, "body": {"error": "{} already exists".format(path.eventsource)}}

    return res


def get_eventsource(db, path, params):
    pass


def delete_eventsource(db, path, params):
    pass
