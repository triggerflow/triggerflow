import json
import os
from uuid import uuid4
from importlib import import_module

from eventprocessor_client.utils import load_config_yaml
from eventprocessor_client.client import CloudEventProcessorClient, CloudEvent, DefaultActions, DefaultConditions
from .conditions_actions import AwsAsfActions, AwsAsfConditions
from dags.dag import DAG

ep = None




def asf2triggers(asf_json):
    global ep
    run_id = 'asf' + '_' + str(uuid4())
    ep_config = load_config_yaml('~/client_config.yaml')
    dags_config = load_config_yaml('~/dags_config.yaml')

    evt_src_type = asf_json['EventSource']
    evt_src_config = dags_config['event_sources'][evt_src_type]
    evt_src_class = dags_config['event_sources'][evt_src_type]['class']
    del evt_src_config['class']

    mod = import_module('eventprocessor_client.sources')
    evt_src = getattr(mod, '{}CloudEventSource'.format(evt_src_class))
    event_source = evt_src(name=run_id,
                           topic=run_id,
                           **evt_src_config)

    ep = CloudEventProcessorClient(api_endpoint=ep_config['event_processor']['api_endpoint'],
                                   user=ep_config['event_processor']['user'],
                                   password=ep_config['event_processor']['password'],
                                   namespace=run_id,
                                   eventsource_name=run_id)

    final_state_name, _ = process_states(asf_json)

    ep.add_trigger(CloudEvent(final_state_name),
                   condition=DefaultConditions.TRUE,
                   action=DefaultActions.TERMINATE)


def process_states(asf_json, iterator=False):
    global ep
    upstream_relatives = {}
    final_states = []

    for state_name, state in asf_json['States'].values():
        upstream_relatives[state['Next']] = state_name

        if 'End' in state and state['End']:
            final_states.append(state)

    last = None, None
    for state_name, state in asf_json['States'].values():
        last = state_name, state
        context = {'subject': state_name, 'iterator': iterator}
        if state['Type'] == 'Pass':
            ep.add_trigger(CloudEvent(upstream_relatives[state_name]),
                           condition=AwsAsfConditions.AWS_ASF_CONDITION,
                           action=AwsAsfActions.AWS_ASF_PASS,
                           context=context)
        elif state['Type'] == 'Task':
            context['resource'] = state['Resource']
            ep.add_trigger(CloudEvent(upstream_relatives[state_name]),
                           condition=AwsAsfConditions.AWS_ASF_CONDITION,
                           action=AwsAsfActions.AWS_ASF_TASK,
                           context=context)
        elif state['Type'] == 'Choice':
            for choice in state['Choices']:
                context['Condition'] = choice.copy()
                ep.add_trigger(CloudEvent(upstream_relatives[choice['Next']]),
                               condition=AwsAsfConditions.AWS_ASF_CONDITION,
                               action=AwsAsfActions.AWS_ASF_CHOICE,
                               context=context)
        elif state['Type'] == 'Parallel':
            leaves = []
            for branch in state['Branches']:
                leaf = process_states(branch, iterator)
                leaves.append(leaf)
            ep.add_trigger([CloudEvent(upstream_relatives[leaf[0]]) for leaf in leaves],
                           condition=AwsAsfConditions.AWS_ASF_CONDITION,
                           action=AwsAsfActions.AWS_ASF_PASS,
                           context=context)
        elif state['Type'] == 'Wait':
            # TODO Ask Pedro about timeouts again
            pass
        elif state['Type'] == 'Map':
            # This won't work
            leaf = process_states(state['Iterator'], True)
            context['iterator'] = True
            ep.add_trigger(CloudEvent(upstream_relatives[leaf[0]]),
                           condition=AwsAsfConditions.AWS_ASF_CONDITION,
                           action=AwsAsfActions.AWS_ASF_PASS,
                           context=context)
        elif state['Type'] == 'Succeed':

            pass
        elif state['Type'] == 'Fail':
            pass

    return last
