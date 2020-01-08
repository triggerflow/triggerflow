import json
from urllib.parse import urlparse

from utils import auth_request


def parse_uri(uri):
    parsed_uri = urlparse(uri)
    if parsed_uri.scheme != 'db' or not parsed_uri.netloc or not parsed_uri.path:
        return False, {"statusCode": 400, "body": {'Bad URI'}}
    else:
        return parsed_uri, None


def put(db, params):
    # Authenticate request
    ok, res = auth_request(db, params)
    if not ok:
        return res

    # Parse URI
    parsed_uri, res = parse_uri(params['uri'])
    if not parsed_uri:
        return res

    db.put(database_name=parsed_uri.netloc, document_id=parsed_uri.path, data=params['data'])
    return {"statusCode": 200, "body": {"created/updated": parsed_uri.netloc+parsed_uri.path}}


def get(db, params):
    # Authenticate request
    ok, res = auth_request(db, params)
    if not ok:
        return res

    # Parse URI
    parsed_uri, res = parse_uri(params['uri'])
    if not parsed_uri:
        return res

    obj = db.get(database_name=parsed_uri.netloc, document_id=parsed_uri.path)
    return {"statusCode": 200, "body": json.dumps(obj)}


def delete(db, params):
    # Authenticate request
    ok, res = auth_request(db, params)
    if not ok:
        return res

    # Parse URI
    parsed_uri, res = parse_uri(params['uri'])
    if not parsed_uri:
        return res

    db.delete(database_name=parsed_uri.netloc, document_id=parsed_uri.path)
    return {"statusCode": 200, "body": {"deleted": parsed_uri.netloc+parsed_uri.path}}
