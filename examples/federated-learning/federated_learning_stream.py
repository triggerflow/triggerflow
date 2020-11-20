import math
import time
import requests

from triggerflow.eventsources.redis import RedisEventSource
from triggerflow.functions import PythonCallable
from triggerflow import EventStream, EventHandler, EventPattern, CloudEvent
from uuid import uuid4
from platform import node
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# Configuration and constants
redis_config = {
    'host': '127.0.0.1',
    'port': 6379,
    'db': 0,
    'password': 'myverysecurepassword'
}

global_context = {
    'ibm_cf_config': {
        'api_key': '123456789',
    }
}

CLIENT_FUNCTION_ENDPOINT = 'https://us-south.functions.cloud.ibm.com/api/v1/namespaces/my_ibmcf_namespace/actions/triggerflow/fedlearn-client'
AGGREGATOR_FUNCTION_ENDPOINT = 'https://us-south.functions.cloud.ibm.com/api/v1/namespaces/my_ibmcf_namespace/actions/triggerflow/fedlearn-aggregator'
TOTAL_CLIENTS = 50


def orchestrator_condition(context, event):
    if 'rounds_completed' not in context:
        context['rounds_completed'] = -1
    context['rounds_completed'] += 1
    return context['rounds_completed'] < context['max_rounds']


def orchestrator_action(context, event):
    payload = {'__OW_TRIGGERFLOW': {'subject': 'aggregator',
                                    'sink': redis_source.get_json_eventsource()},
               'task': 'train',
               'total_clients': context['total_clients'],
               'round': event['data']['round']}

    if 'aggregate_result_key' in event['data']:
        payload['weights_key'] = event['data']['aggregate_result_key']

    url = context['client_endpoint']

    payloads = []
    for i in range(context['total_clients']):
        p = payload.copy()
        p['client_id'] = i
        payloads.append(p)

    auth = tuple(context.global_context['ibm_cf_config']['api_key'].split(':'))
    with ThreadPoolExecutor() as pool:
        responses = pool.map(lambda data: requests.post(url, json=data, auth=auth), payloads)

    resp = list(responses)
    all([response.ok for response in responses])


def aggregator_condition(context, event):
    if 'data' in event and 'result' in event['data'] and event['data']['result'] == 'Reject':
        return False
    elif event['type'] == 'event.triggerflow.termination.success':
        context['result_keys'].append(event['data']['result_key'])
        event_round = event['data']['round']
        if event_round not in context['counter']:
            context['counter'][event_round] = 0
        context['counter'][event_round] += 1
        return context['counter'][event_round] == math.ceil(context['total_clients'] * context['threshold'])
    elif event['type'] == 'event.triggerflow.timeout':
        return True
    else:
        return False


def aggregator_action(context, event):
    if event['type'] == 'event.triggerflow.timeout':
        event_uuid = uuid4()
        reset_round_event = {'specversion': '1.0',
                             'id': event_uuid.hex,
                             'source': f'urn:{node()}:{str(event_uuid)}',
                             'type': 'round_start.federated_learning.triggerflow',
                             'time': str(datetime.utcnow().isoformat()),
                             'subject': 'orchestrator',
                             'datacontenttype': 'application/json',
                             'round': context['round']}
        context.local_event_queue.put(reset_round_event)
    else:
        payload = {'__OW_TRIGGERFLOW': {'subject': 'orchestrator',
                                        'sink': redis_source.get_json_eventsource()},
                   'result_keys': context['result_keys'],
                   'round': context['round']}

        url = context['aggregator_endpoint']
        auth = tuple(context.global_context['ibm_cf_config']['api_key'].split(':'))
        print('### AGGREGATOR ### $ {}'.format(time.time()))
        requests.post(url, json=payload, auth=auth)
        context['round'] += 1


redis_source = RedisEventSource(**redis_config)

# Setup event triggers
EventStream(redis_source, global_context).match({
    EventPattern(subject=r'^orchestrator$', type=r'.*'):
        EventHandler(condition=PythonCallable(orchestrator_condition),
                     action=PythonCallable(orchestrator_action),
                     context={'round': 1,
                              'client_endpoint': CLIENT_FUNCTION_ENDPOINT,
                              'total_clients': TOTAL_CLIENTS,
                              'max_rounds': 3}),
    EventPattern(subject=r'^aggregator$', type=r'.*'):
        EventHandler(condition=PythonCallable(aggregator_condition),
                     action=PythonCallable(aggregator_action),
                     context={'round': 1,
                              'result_keys': [],
                              'counter': {},
                              'threshold': .65,
                              'aggregator_endpoint': AGGREGATOR_FUNCTION_ENDPOINT,
                              'total_clients': TOTAL_CLIENTS})
})

# Fire 'orchestrator' trigger manually and start the process
round_start_event = CloudEvent().SetEventType('round_start.federated_learning.triggerflow').SetSubject('orchestrator')
round_start_event.SetData({'round': 1, 'task': 'train'})
redis_source.publish_cloudevent(round_start_event)
