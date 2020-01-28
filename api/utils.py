import secrets
import dateutil.parser
import types
import re
from base64 import b64decode
from datetime import datetime

TOKEN_LEN = 32


def parse_path(path):
    path = path.split('/')
    ppath = types.SimpleNamespace()
    if 'namespace' in path:
        ppath.namespace = path[path.index('namespace') + 1] if path.index('namespace') + 1 < len(path) else None
    if 'eventsource' in path:
        ppath.eventsource = path[path.index('eventsource') + 1] if path.index('eventsource') + 1 < len(path) else None
    if 'trigger' in path:
        ppath.trigger = path[path.index('trigger') + 1] if path.index('trigger') + 1 < len(path) else None
    return ppath


def authenticate_request(db, params):
    try:
        if 'authorization' not in params['headers']:
            return False, {'statusCode': 401, 'body': {'error': "Unauthorized"}}

        user, password = get_authentication_parameters(params)

        if not re.fullmatch(r"[a-zA-Z0-9_]+", user) or not re.fullmatch(r"^(?=.*[A-Za-z])[A-Za-z\d@$!%*#?&]+$",
                                                                        password):
            return False, {'statusCode': 401, 'body': {'error': "Invalid user:password"}}

        users = db.get(database_name='auth$', document_id='users')

        ok = user in users and password == users[user]
        return ok, None

    except KeyError or ValueError or IndexError as e:
        return False, {'statusCode': 400, 'body': {'error': str(e)}}


def get_authentication_parameters(params):
    encoded_userpasswd = params['headers']['authorization'].split(' ')[1]
    decoded_userpasswd = b64decode(encoded_userpasswd.encode('utf-8')).decode('utf-8')

    if decoded_userpasswd.count(':') > 1:
        return False, {'statusCode': 401, 'body': {'error': "Unauthorized"}}

    return tuple(decoded_userpasswd.split(':'))
