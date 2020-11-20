from sklearn.linear_model import SGDClassifier
from sklearn.datasets import fetch_20newsgroups_vectorized
from scipy import sparse
from sklearn.preprocessing import normalize
import pickle
import ibm_botocore
import ibm_boto3
import logging
import random
import time
import numpy as np

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def load_data(subset, idx, n):
    X, y = fetch_20newsgroups_vectorized(subset=subset, return_X_y=True)

    # start = X.shape[0] // n * idx
    # end = X.shape[0] // n * (idx + 1)
    #
    # return X[start:end], y[start:end]
    return X, y


def fit(X, y, coef=None, intercept=None):
    clf = SGDClassifier(fit_intercept=True)

    if None in [coef, intercept]:
        # Initial round
        clf.fit(X, y)
        return sparse.csr_matrix(clf.coef_), sparse.csr_matrix(clf.intercept_)
    else:
        coef = coef.todense()
        intercept = np.asarray(intercept.todense().tolist()[0])
        clf.fit(X, y, coef_init=coef, intercept_init=intercept)

        delta_coef = sparse.csr_matrix(clf.coef_ - coef)
        delta_intercept = sparse.csr_matrix(clf.intercept_ - intercept)
        return delta_coef, delta_intercept


def test(X, y, coef, intercept):
    clf = SGDClassifier(fit_intercept=True)
    clf.coef_ = coef.todense()
    clf.intercept_ = intercept.todense()
    clf.classes_ = np.unique(y)

    return clf.score(X, y)


def main(args):
    logger.debug(args)

    task = args['task']
    client_id = args['client_id']
    round_no = args['round']
    total_clients = args['total_clients']

    if 0 <= random.random() < 0.35:
        # Simulate a faulty client that rejects training for this round
        return {'result': 'Reject', 'round': round_no, 'client_id': client_id}
    else:
        # Simulate client heterogeneity
        sleep_seconds = random.random() * (random.random() * 50)
        logger.debug('Going to sleep for {} seconds'.format(sleep_seconds))
        time.sleep(sleep_seconds)

    logger.debug('Initialize COS client')
    client_config = ibm_botocore.client.Config(signature_version='oauth',
                                               max_pool_connections=128)
    cos = ibm_boto3.client('s3',
                           ibm_api_key_id=args['ibm_api_key_id'],
                           ibm_service_instance_id=args['ibm_service_instance_id'],
                           config=client_config,
                           endpoint_url=args['cos_endpoint_url'])

    X, y = load_data('train', client_id, total_clients)

    weights_key = args.get('weights_key', None)
    if weights_key is not None:
        logger.debug('Going to download current weights from COS: {}'.format(weights_key))
        obj = cos.get_object(Key=weights_key, Bucket=args['bucket'])
        coef, intercept = pickle.loads(obj['Body'].read())
        logger.debug('Ok')
    else:
        logger.debug('First round, there are no weights yet')
        coef, intercept = None, None

    if task == 'train':
        logger.debug('Training...')
        result = fit(X, y, coef, intercept)
    elif task == 'test':
        logger.debug('Testing model...')
        result = test(X, y, coef, intercept)
    else:
        logger.debug('Task was {}'.format(task))
        result = None

    obj = pickle.dumps(result)
    obj_key = 'results/round_{}/{}.result'.format(round_no, client_id)
    logger.debug('Going to upload result as {} (Total length: {} bytes)'.format(obj_key, len(obj)))
    cos.put_object(Key=obj_key, Bucket=args['bucket'], Body=obj)
    logger.debug('Ok')

    return {'client_id': client_id, 'result_key': obj_key, 'round': round_no}
