import sys
import json
import pickle
from triggerflow import CloudEvent
from triggerflow.eventsources.redis import RedisEventSource
from cloudbutton.config import extract_storage_config
from cloudbutton.cloud_proxy import CloudStorage, CloudFileProxy


def aggregate(results, curr_coef, curr_intercept):
    dcoefs = [delta_coef for delta_coef, delta_intercept in results]
    dintercepts = [delta_intercept for delta_coef, delta_intercept in results]
    avg_dcoefs = sum(dcoefs) / len(dcoefs)
    avg_dintercepts = sum(dintercepts) / len(dintercepts)

    return (
        curr_coef + avg_dcoefs,
        curr_intercept + avg_dintercepts,
        )
    

def main(args):
    task = args['task']
    task_result_keys = args['task_result_keys']
    agg_result_key = args['agg_result_key']
    os = CloudFileProxy(CloudStorage(args['cb_config']))
    open = os.open

    # Load stored client results
    task_results = []
    for k in task_result_keys:
        with open(k, 'rb') as f:
            task_results.append(pickle.loads(f.read()))

    # Aggregate
    if task == 'train':
        current_weights_key = args['current_weights_key']
        if os.path.exists(current_weights_key):
            with open(current_weights_key, 'rb') as f:
                curr_coef, curr_intercept = pickle.loads(f.read())
        else:
            curr_coef, curr_intercept = (0, 0)

        agg_result = aggregate(task_results, curr_coef, curr_intercept)

    if task == 'test':
        agg_result = sum(task_results) / len(task_results)

    # Store result
    with open(agg_result_key, 'wb') as f:
        f.write(pickle.dumps(agg_result))

    # Delete client results
    [os.remove(k) for k in task_result_keys]

    redis_source = RedisEventSource(**args['cb_config']['redis'], stream='fedlearn')
    event = CloudEvent().SetEventType('aggregation_complete').SetSubject('fedlearn.aggregator')
    redis_source.publish_cloudevent(event)

    return {'success': 1}


if __name__ == "__main__":
    main(sys.argv[1])

