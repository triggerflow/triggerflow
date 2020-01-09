import secrets
import dateutil.parser
import types
from datetime import datetime

from ibm_cloudfunctions_client import CloudFunctionsClient

TOKEN_LEN = 32

def parse_path(path):
    path = path.split('/')
    ppath = types.SimpleNamespace()
    if 'namespace' in path:
        ppath.namespace = path[path.index('namespace')+1]
    if 'eventsource' in path:
        ppath.namespace = path[path.index('namespace')+1]
    if 'trigger' in path:
        ppath.namespace = path[path.index('namespace') + 1]
    return ppath




def gen_token(db, params):
    try:
        ibm_cf_credentials = params['authentication']['ibm_cf_credentials']
        ok = check_cloudfunctions_credentials(**ibm_cf_credentials)
        if not ok:
            raise Exception('Invalid IBM Cloud Functions credentials')
    except KeyError:
        return {'statusCode': 400, 'body': {'error': 'Missing parameters'}}
    except Exception as e:
        return {'statusCode': 400, 'body': {'error': str(e)}}

    # Generate session token
    token = secrets.token_hex(TOKEN_LEN)
    timestamp = str(datetime.utcnow().isoformat())

    try:
        sessions = db.get(database_name='auth__', document_id='sessions')
    except KeyError:
        sessions = {}
    sessions[token] = timestamp
    db.put(database_name='auth__', document_id='sessions', data=sessions)

    return {'statusCode': 200, 'body': {'token': token}}


def check_cloudfunctions_credentials(endpoint, namespace, api_key):
    try:
        cf_client = CloudFunctionsClient(endpoint=endpoint,
                                         namespace=namespace,
                                         api_key=api_key)
        cf_client.list_packages()
        return True
    except Exception:
        return False


def auth_request(db, params):
    if 'authorization' not in params['headers']:
        return False, {'statusCode': 401, 'body': {'error': "Unauthorized"}}
    token = params['headers']['authorization'].split(' ')[1]
    print(token)
    sessions = db.get(database_name='auth__', document_id='sessions')
    if token in sessions:
        emitted_token_time = dateutil.parser.parse(sessions[token])
        seconds = (datetime.utcnow() - emitted_token_time).seconds
        if seconds > 3600:  # 1 hour
            return False, {'statusCode': 403, 'body': {'error': 'Token expired'}}
        else:
            return True, None
    else:
        return False, {'statusCode': 401, 'body': {'error': 'Unauthorized'}}
