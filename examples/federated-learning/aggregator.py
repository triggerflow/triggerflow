import numpy as np
import logging
import pickle
import ibm_botocore
import ibm_boto3

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def main(args):
    logger.debug(args)

    logger.debug('Initialize COS client')
    client_config = ibm_botocore.client.Config(signature_version='oauth',
                                               max_pool_connections=128)
    cos = ibm_boto3.client('s3',
                           ibm_api_key_id=args['ibm_api_key_id'],
                           ibm_service_instance_id=args['ibm_service_instance_id'],
                           config=client_config,
                           endpoint_url=args['cos_endpoint_url'])

    round_no = args['round']
    logger.debug('Round: {}'.format(round_no))

    weights_key = args.get('weights_key', None)
    if weights_key is not None:
        logger.debug('Going to download current weights from COS: {}'.format(weights_key))
        obj = cos.get_object(Key=weights_key, Bucket=args['bucket'])
        coef, intercept = pickle.loads(obj['Body'].read())
        logger.debug('Ok')
    else:
        logger.debug('First round, there are no weights yet')
        coef, intercept = 0, 0

    logger.debug('There are {} result keys'.format(len(args['result_keys'])))
    results = []

    for client_result_key in args['result_keys']:
        logger.debug('Going to download {}'.format(client_result_key))
        obj = cos.get_object(Bucket=args['bucket'], Key=client_result_key)
        delta_coef, delta_intercept = pickle.loads(obj['Body'].read())
        results.append((delta_coef, delta_intercept))

    logger.debug(results)
    dcoefs = [delta_coef for delta_coef, _ in results]
    dintercepts = [delta_intercept for _, delta_intercept in results]
    logger.debug(dcoefs)
    logger.debug(dintercepts)
    avg_dcoefs = sum(dcoefs) / len(dcoefs)
    avg_dintercepts = sum(dintercepts) / len(dintercepts)

    agg_result = (avg_dcoefs + coef, avg_dintercepts + intercept)

    agg_result_key = 'agg_{}'.format(round_no)
    obj = pickle.dumps(agg_result)
    logger.debug('Going to upload aggregate result: {} ({} bytes)'.format(agg_result_key, len(obj)))
    cos.put_object(Bucket=args['bucket'], Key=agg_result_key, Body=obj)
    logger.debug('Ok')

    next_round = round_no + 1
    return {'aggregate_result_key': agg_result_key, 'round': next_round}
