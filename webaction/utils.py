import secrets

from ibm_cloudfunctions_client import CloudFunctionsClient

MANDATORY_PARAMS = {'namespace', 'event_source', 'authentication', 'private_credentials'}


def validate_params(params):
    # Check credentials
    if not MANDATORY_PARAMS.issubset(set(params)):
        raise Exception("Missing mandatory parameters")

    # Check Cloudant credentials
    if 'cloudant' not in params['private_credentials'] and \
            not {'apikey', 'username'}.issubset(params['private_credentials']['cloudant']):
        raise Exception('Invalid private credentials')

    # Delete extra param keys
    valid_params = params.copy()
    for k in params.keys():
        if '__ow' in k:
            del valid_params[k]
    return valid_params


def check_cloudfunctions_credentials(endpoint, namespace, api_key):
    try:
        cf_client = CloudFunctionsClient(endpoint=endpoint,
                                         namespace=namespace,
                                         api_key=api_key)
        cf_client.list_packages()
        return True
    except Exception:
        return False


def generate_token(len=32):
    return secrets.token_hex(len)
