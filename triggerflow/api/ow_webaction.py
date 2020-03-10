import re

import triggers
import workspaces
import eventsources
from redis_db import RedisClient
from cloudant_db import CloudantClient
from utils import authenticate_request

import traceback  # debug


def ow_webaction_main(args):
    print(args)  # DEBUG

    if args['__ow_headers']['content-type'] != 'application/json':
        return {"statusCode": 400, "body": {"error": "Bad request"}}

    params = args.copy()
    params['headers'] = args['__ow_headers'].copy()
    path = args['__ow_path']

    print('-------------')
    print(params)

    try:
        # Instantiate database client
        #db = CloudantClient(**params['credentials']['cloudant'])
        db = RedisClient(**params['credentials']['redis'])

        # Authorize request
        ok, res = authenticate_request(db, params)
        if not ok:
            return {'statusCode': 401, 'body': {'error': "Unauthorized"}}

        if re.fullmatch(r"/workspace/[^/]+", path):
            if args['__ow_method'] == 'put':
                res = workspaces.add_workspace(db, path, params)
            elif args['__ow_method'] == 'get':
                res = workspaces.get_workspace(db, path, params)
            elif args['__ow_method'] == 'delete':
                res = workspaces.delete_workspace(db, path, params)

        elif re.fullmatch(r"/workspace/[^/]+/eventsource", path):
            if args['__ow_method'] == 'get':
                res = eventsources.list_eventsources(db, path, params)

        elif re.fullmatch(r"/workspace/[^/]+/eventsource/[^/]+", path):
            if args['__ow_method'] == 'put':
                res = eventsources.add_eventsource(db, path, params)
            elif args['__ow_method'] == 'get':
                res = eventsources.get_eventsource(db, path, params)
            elif args['__ow_method'] == 'delete':
                res = eventsources.delete_eventsource(db, path, params)

        elif re.fullmatch(r"/workspace/[^/]+/trigger", path):
            if args['__ow_method'] == 'post':
                res = triggers.add_trigger(db, path, params)

        elif re.fullmatch(r"/workspace/[^/]+/trigger/[^/]+", path):
            if args['__ow_method'] == 'get':
                res = triggers.get_trigger(db, path, params)
            elif args['__ow_method'] == 'delete':
                res = triggers.delete_trigger(db, path, params)
        else:
            res = {"statusCode": 400, "body": {"error": "Bad request"}}

        return res
    except KeyError as e:
        print(traceback.format_exc())  # debug
        return {"statusCode": 400, "body": {'error': 'Key error: {}'.format(str(e))}}
    except Exception as e:
        print(traceback.format_exc())  # debug
        return {"statusCode": 500, "body": {'error': "Internal error: {}".format(str(e))}}
