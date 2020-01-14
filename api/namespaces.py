import re
from utils import auth_request, parse_path


def add_namespace(db, path, params):
    ok, res = auth_request(db, params)
    if not ok:
        return res

    path = parse_path(path)
    if db.exists(database_name=path.namespace):
        return {"statusCode": 409, "body": {"error": "Namespace {} already exists".format(path.namespace)}}
    elif not re.fullmatch(r"^[a-zA-Z0-9_.-]*$", path.namespace):
        return {"statusCode": 400, "body": {"error": "Illegal namespace name".format(path.namespace)}}

    db.put(database_name=path.namespace, document_id='.event_sources', data={})
    db.put(database_name=path.namespace, document_id='.trigger_events', data={})
    db.put(database_name=path.namespace, document_id='.triggers', data={})
    db.put(database_name=path.namespace, document_id='.global_context', data=params['global_context'])
    return {"statusCode": 201, "body": {"created": path.namespace}}


def get_namespace(db, path, params):
    pass


def delete_namespace(db, path, params):
    pass
