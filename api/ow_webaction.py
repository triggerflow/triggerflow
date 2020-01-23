import re

import triggers
import namespaces
import eventsources
from cloudant_client import CloudantClient
from utils import authenticate_request


def ow_webaction_main(args):
    print(args)  # DEBUG

    if args['__ow_headers']['content-type'] != 'application/json':
        return {"statusCode": 400, "body": {"error": "Bad request"}}

    params = args.copy()
    params['headers'] = args['__ow_headers'].copy()
    path = args['__ow_path']

    try:

        # Instantiate database client
        db = CloudantClient(username=params['private_credentials']['cloudant']['username'],
                            apikey=params['private_credentials']['cloudant']['apikey'])

        # Authorize request
        ok, res = authenticate_request(db, params)
        if not ok:
            return res

        if re.fullmatch(r"/namespace/[^/]+", path):
            if args['__ow_method'] == 'put':
                res = namespaces.add_namespace(db, path, params)
            elif args['__ow_method'] == 'get':
                res = namespaces.get_namespace(db, path, params)
            elif args['__ow_method'] == 'delete':
                res = namespaces.delete_namespace(db, path, params)
        elif re.fullmatch(r"/namespace/[^/]+/eventsource/[^/]+", path):
            if args['__ow_method'] == 'put':
                res = eventsources.add_eventsource(db, path, params)
            elif args['__ow_method'] == 'get':
                res = eventsources.get_eventsource(db, path, params)
            elif args['__ow_method'] == 'delete':
                res = eventsources.delete_eventsource(db, path, params)
        elif re.fullmatch(r"/namespace/[^/]+/trigger", path):
            if args['__ow_method'] == 'post':
                res = triggers.add_trigger(db, path, params)
        elif re.fullmatch(r"/namespace/[^/]+/trigger/[^/]+", path):
            if args['__ow_method'] == 'get':
                res = triggers.get_trigger(db, path, params)
            elif args['__ow_method'] == 'delete':
                res = triggers.delete_trigger(db, path, params)
        else:
            res = {"statusCode": 400, "body": {"error": "Bad request"}}

        return res
    except KeyError as e:
        print(e)  # debug
        return {"statusCode": 400, "body": {'error': 'Key error: {}'.format(str(e))}}
    except Exception as e:
        print(e)  # debug
        return {"statusCode": 500, "body": {'error': "Internal error: {}".format(str(e))}}
