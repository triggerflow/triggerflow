import time


def main(args):
    if 'sleep' in args:
        time.sleep(args['sleep'])
    return {'result': args.get('echo', None)}
