from sklearn.linear_model import SGDClassifier
from sklearn.datasets import fetch_20newsgroups_vectorized
import numpy as np
from scipy import sparse
import time
import pickle
from cloudbutton.cloud_proxy import open, os
from cloudbutton.multiprocessing.util import get_uuid
from triggerflow import CloudEvent
from triggerflow.eventsources.redis import RedisEventSource
from cloudbutton.config import default_config


def load_data(subset, idx, n):
    X, y = fetch_20newsgroups_vectorized(subset=subset, return_X_y=True)

    start = X.shape[0] // n * idx
    end = X.shape[0] // n * (idx + 1)

    return X[start:end], y[start:end]

def fit(X, y, coef, intercept):
    clf = SGDClassifier(fit_intercept=True)

    if coef is None:
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


def main(lock, model_state):
    place = None
    while place is None:
        # Attempt to participate in the training round
        with lock:
            state = model_state.value
            interval = state['interval']

            # A place will be obtained if:
            #   - there are free places to take (timestamp == 0)
            #   - some client has not completed its training within the interval
            oldest = 0
            t_now = time.time()
            for i, timestamp in enumerate(state['round_ts']):
                if timestamp == -1:
                    continue

                t_elapsed = t_now - timestamp
                if t_elapsed > interval:
                    place = i
                    break

                if t_elapsed > oldest:
                    oldest = t_elapsed

            if place is not None:
                # Take this place by putting the current timestamp
                state['round_ts'][place] = t_now
                model_state.value = state
                print('Acquired place:', place, '|', state['round_ts'])

        if place is None:
            # Retry when the interval of the oldest client training has expired
            print('Sleeping for:', interval - oldest)
            time.sleep(interval - oldest)

    task = state['task']    # 'train' or 'test'
    n = len(state['round_ts'])
    X, y = load_data(task, place, n)

    if os.path.exists(state['current_weights_key']):
        with open(state['current_weights_key'], 'rb') as f:
            coef, intercept = pickle.loads(f.read())
    else:
        coef, intercept = None, None
    
    if task == 'train':
        result = fit(X, y, coef, intercept)

    if task == 'test':
        result = test(X, y, coef, intercept)

    lock.acquire()
    state = model_state.value
    # If our place was not revoked
    # (could have taken too long to train)
    if state['round_ts'][place] == t_now:
        # Mark as completed
        state['round_ts'][place] = -1
        print('Task done, place:', place, '|', state['round_ts'])

        # Store result
        result_key = get_uuid()
        with open(result_key, 'wb') as f:
            f.write(pickle.dumps(result))
        
        # If the round is not complete, release the lock and continue
        if state['round_ts'].count(-1) != len(state['round_ts']):
            model_state.value = state
            lock.release()
        # Otherwise the lock will be released when the aggregator
        # finishes and the next round starts

        # Send task complete event with the result key
        redis_source = RedisEventSource(**default_config()['redis'], stream='fedlearn')
        event = CloudEvent().SetEventType('client_task_result').SetSubject('fedlearn.client')
        event.SetData({
            'result_key': result_key,
            'task': task
        })
        redis_source.publish_cloudevent(event)
        print('Result event sent')

    else:
        # If we surpassed the interval and lost our place
        # repeat the process until we succesfully contribute
        main(lock, model_state)



        

    
