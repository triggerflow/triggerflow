#!/usr/bin/env python
# coding: utf-8

# # Federated learning example using Triggerflow and Cloudbutton
# 
# In this example, we have leveraged the flexibility of Triggerflow to implement a federated learning workflow using triggers. We use Triggerflow's triggers as a mechanism to accumulate and aggregate partial updates of the model from each client. The system is designed as a cyclic process where training rounds take place and a final aggregation phase updates the model and restarts the cycle. We also use the [Cloudbutton Toolkit](https://github.com/cloudbutton/cloudbutton) for storing a shared state, storing partial results and synchronizing.
# 
# **See the [client](/client.py) and the [aggregator](aggregator/aggregator.py)**

# In[3]:


from triggerflow.eventsources.redis import RedisEventSource
from triggerflow.functions import PythonCallable, python_object
from triggerflow import Triggerflow, CloudEvent, DefaultConditions, DefaultActions

from cloudbutton.config import default_config
from cloudbutton.multiprocessing import Manager
from cloudbutton.cloud_proxy import os as cloudfs

from client import main as client
from concurrent.futures import ThreadPoolExecutor
import pickle
import subprocess
import os
import time


# ### Deploy aggregator action
# Fill `ibm_cf` parameter in `./.cloudbutton_config` file with IBM credentials

# In[13]:


cb_config = default_config()
ibm_config = cb_config['ibm_cf']

os.chdir('aggregator')
aggregator_url = subprocess.check_output(['python3', 'create_action.py',
    '--endpoint', ibm_config['endpoint'],
    '--namespace', ibm_config['namespace'],
    '--api-key', ibm_config['api_key']]).decode()
os.chdir('..')


# As an event source, we use the same Redis server found in Cloudbutton's configuration file because the toolkit already uses it for the manager objects.

# In[14]:


# Instantiate Triggerflow client
tf_client = Triggerflow()

# Create a workspace and add a Redis event source to it
redis_source = RedisEventSource(**cb_config['redis'], stream='fedlearn')
tf_client.create_workspace(workspace_name='fedlearn', event_source=redis_source)


# In[15]:


# Initialize state/round
ROUND_N = 5
manager = Manager()
manager.start()
lock = manager.Lock()
model_state = manager.Value()
state = {
    'task': 'train',
    'interval': 5,
    'round_table': [0] * ROUND_N,
    'current_weights_key': 'model_weights',
    'iter_count': 0
}
model_state.value = state


# ## Aggregation trigger

# In[17]:


# Create the trigger activation event 
client_act_event = CloudEvent().SetEventType('client_task_result').SetSubject('fedlearn.client')

# Create a custom Python callable condition
def custom_join(context, event):
    context['task_result_keys'].append(event['data']['result_key'])
    context['task'] = event['data']['task']

    if len(context['task_result_keys']) == context['join']:
        context['invoke_kwargs'] = {
            'task': context['task'],
            'cb_config': context['cb_config'],
            'task_result_keys': context['task_result_keys'],
            'current_weights_key': context['current_weights_key'],
        }
        if context['task'] == 'train':
            context['invoke_kwargs']['agg_result_key'] = context['current_weights_key']
        else:
            context['invoke_kwargs']['agg_result_key'] = 'model_score'
        context['task_result_keys'] = []
        return True
    return False

# Create a trigger with the custom condition
tf_client.add_trigger(
    trigger_id='aggregation_trigger',
    event=client_act_event,
    condition=PythonCallable(custom_join),
    action=DefaultActions.IBM_CF_INVOKE,
    transient=False,
    context={
        'url': aggregator_url,
        'api_key': ibm_config['api_key'],
        'cb_config': cb_config,
        'task_result_keys': [],
        'current_weights_key': state['current_weights_key'],
        'join': ROUND_N,
    })


# ## Round restart trigger
# 

# In[18]:


# Create the trigger activation event 
aggregator_act_event1 = CloudEvent().SetEventType('aggregation_complete').SetSubject('fedlearn.aggregator')
aggregator_act_event2 = CloudEvent().SetEventType('change_task').SetSubject('fedlearn.aggregator')

# Create a custom Python callable action
def prepare_round(context, event):
    if event['type'] == 'change_task':
        context['task'] = event['data']['task']
    else:
        # Reset round
        state = context['model_state'].value
        state['round_table'] = [0] * ROUND_N
        if state['task'] == 'train':
            state['iter_count'] += 1
        state['task'] = context['task']

        context['model_state'].value = state
        context['lock'].release()

# Create a trigger with the custom action
tf_client.add_trigger(
    trigger_id='round_restart_trigger',
    event=[aggregator_act_event1, aggregator_act_event2],
    condition=DefaultConditions.TRUE,
    action=PythonCallable(prepare_round),
    transient=False,
    context={
        'lock': python_object(lock),
        'model_state': python_object(model_state),
        'task': 'train'
    })


# ## Client simulation

# In[ ]:


# Pre-fetch dataset locally
from sklearn.datasets import fetch_20newsgroups_vectorized
fetch_20newsgroups_vectorized()

# Launch simulated clients
NUM_CLIENTS = 10
with ThreadPoolExecutor() as executor:
    executor.map(lambda x: client(*x), [[lock, model_state]] * NUM_CLIENTS)

# Run a test round
change_task_event = CloudEvent().SetEventType('change_task').SetSubject('fedlearn.aggregator')
change_task_event.SetData({
    'task': 'test'
})
redis_source.publish_cloudevent(change_task_event)

with ThreadPoolExecutor() as executor:
    executor.map(lambda x: client(*x), [[lock, model_state]] * NUM_CLIENTS)

while not cloudfs.path.exists('model_score'):
    time.sleep(0.5)
with cloudfs.open('model_score', 'rb') as f:
    score = pickle.loads(f.read())

print('Done!\n\n')
print('Training iterations:', model_state.value['iter_count'])
print('Model score:', score)


# Note: in this example clients store their results into Redis that serves as a cloud storage backend apart from serving as a cache for the shared state (model_state) and the synchronization utilities (lock). We do this to avoid fiddling with more credentials, since in a common use case we would use a serverless object storage (AWS S3, IBM COS, GCP Storage) where loads of results could be stored and accessed massively. More into how to configure Cloudbutton's storage backends [here](https://github.com/cloudbutton/cloudbutton/tree/master/config).
