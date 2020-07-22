from triggerflow.eventsources.redis import RedisEventSource
from triggerflow.functions import PythonCallable, python_object
from triggerflow import Triggerflow, CloudEvent, DefaultConditions, DefaultActions

from cloudbutton.config import default_config
from cloudbutton.multiprocessing import Manager

from client import main as client
from concurrent.futures import ThreadPoolExecutor

aggregator_url = ''
ibm_cf_api_key = ''

# Instantiate Triggerflow client
tf_client = Triggerflow()

# Create a workspace and add a Redis event source to it
cb_config = default_config()
redis_source = RedisEventSource(**cb_config['redis'], stream='fedlearn')
tf_client.create_workspace(workspace_name='fedlearn', event_source=redis_source)

# Initialize state/round
ROUND_N = 5
manager = Manager()
manager.start()
lock = manager.Lock()
model_state = manager.Value()
state = {
    'task': 'train',
    'interval': 5,
    'round_ts': [0] * ROUND_N,
    'current_weights_key': 'model_weights',
    'iter_num': 0
}
model_state.value = state

# ==================================================
# Client trigger

# Create the trigger activation event 
client_act_event = CloudEvent().SetEventType('client_task_result').SetSubject('fedlearn.client')

# Create a custom Python callable condition
def custom_join(context, event):
    context['task_result_keys'].append(event['data']['result_key'])
    context['task'] = event['data']['task']

    if len(context['task_result_keys']) == context['join']:
        context.pop('invoke_kwargs', None)
        context['invoke_kwargs'] = context.copy()
        if context['task'] == 'train':
            context['invoke_kwargs']['agg_result_key'] = context['current_weights_key']
        else:
            context['invoke_kwargs']['agg_result_key'] = 'model_score'
        context['task_result_keys'] = []
        return True
    return False

# Create a trigger with the custom condition
tf_client.add_trigger(
    trigger_id='round_join',
    event=client_act_event,
    condition=PythonCallable(custom_join),
    action=DefaultActions.IBM_CF_INVOKE,
    transient=False,
    context={
        'url': aggregator_url,
        'api_key': ibm_cf_api_key,
        #'sink': {},
        #'invoke_kwargs': {},
        'cb_config': cb_config,
        'task_result_keys': [],
        'current_weights_key': state['current_weights_key'],
        'join': ROUND_N,
    })

# ==================================================
# Aggregator trigger

# Create the trigger activation event 
aggregator_act_event = CloudEvent().SetEventType('aggregation_complete').SetSubject('fedlearn.aggregator')

# Create a custom Python callable action
def prepare_round(context, event):
    if event['type'] == 'fedlearn.change_task':
        context['task'] = event['data']['task']
    else:
        # Reset round
        state = context['model_state'].value
        state['round_ts'] = [0] * ROUND_N
        state['task'] = context['task']
        state['iter_num'] += 1

        context['model_state'].value = state
        context['lock'].release()

# Create a trigger with the custom action
tf_client.add_trigger(
    trigger_id='round_prepare',
    event=aggregator_act_event,
    condition=DefaultConditions.TRUE,
    action=PythonCallable(prepare_round),
    transient=False,
    context={
        'lock': python_object(lock),
        'model_state': python_object(model_state),
        'task': 'train'
    })


# ==================================================
# Client simulation

# Pre-fetch dataset locally
from sklearn.datasets import fetch_20newsgroups_vectorized
fetch_20newsgroups_vectorized()

# Launch simulated clients
NUM_CLIENTS = 10
with ThreadPoolExecutor() as executor:
    executor.map(lambda x: client(*x), [[lock, model_state]] * NUM_CLIENTS)
