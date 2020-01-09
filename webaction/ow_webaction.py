import utils
import methods
import storage
from cloudant_client import CloudantClient


def ow_webaction_main(args):
    print(args)  # DEBUG

    if args['__ow_headers']['content-type'] != 'application/json':
        return {"statusCode": 400, "body": {"error": "Bad request"}}

    params = args.copy()
    params['headers'] = args['__ow_headers'].copy()

    print(params)

    try:
        db = CloudantClient(username=params['private_credentials']['cloudant']['username'],
                            apikey=params['private_credentials']['cloudant']['apikey'])

        if args['__ow_path'] == '/init':
            if args['__ow_method'] == 'put':
                res = methods.init(db, params)
        elif args['__ow_path'] == '/auth':
            if args['__ow_method'] == 'get':
                res = utils.gen_token(db, params)
        elif args['__ow_path'] == '/add_trigger':
            if args['__ow_method'] == 'put':
                res = methods.add_trigger(db, params)
        elif args['__ow_path'] == '/db':
            if args['__ow_method'] == 'put':
                res = storage.put(db, params)
            elif args['__ow_method'] == 'get':
                res = storage.get(db, params)
            elif args['__ow_method'] == 'delete':
                res = storage.delete(db, params)
        else:
            res = {"statusCode": 404, "body": {"error": "Not found"}}
        return res
    except KeyError as e:
        raise e # debug
        return {"statusCode": 400, "body": {'error': 'Key error: {}'.format(str(e))}}
    except Exception as e:
        raise e  # debug
        return {"statusCode": 500, "body": {'error': "Internal error: {}".format(str(e))}}