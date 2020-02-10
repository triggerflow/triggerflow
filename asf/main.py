from uuid import uuid4
from importlib import import_module

from eventprocessor_client.utils import load_config_yaml
from eventprocessor_client.client import CloudEventProcessorClient, CloudEvent, DefaultActions, DefaultConditions
from asf.conditions_actions import AwsAsfActions, AwsAsfConditions

ep = None
sm = 0


def asf2triggers(asf_json):
    global ep, sm
    # run_id = '_'.join(['asf_state-machine', str(uuid4())])
    run_id = 'asf_test'
    ep_config = load_config_yaml('~/client_config.yaml')
    asf_config = load_config_yaml('~/asf_config.yaml')

    evt_src_type = asf_config['event_source']
    evt_src_config = asf_config['event_sources'][evt_src_type]
    evt_src_class = asf_config['event_sources'][evt_src_type]['class']
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

    # ep.create_namespace(run_id, event_source=event_source)

    state_machine(asf_json)




def state_machine(asf_json, iterator=False):
    global ep
    upstream_relatives = {}
    final_states = []

    for state_name, state in asf_json['States'].items():
        if 'End' in state and state['End']:
            final_states.append(state_name)
        else:
            upstream_relatives[state['Next']] = state_name

    upstream_relatives[asf_json['StartAt']] = '$init'

    for state_name, state in asf_json['States'].items():
        context = {'subject': state_name, 'iterator': iterator}
        if state['Type'] == 'Pass':
            ep.add_trigger(CloudEvent(upstream_relatives[state_name]),
                           condition=AwsAsfConditions.AWS_ASF_CONDITION,
                           action=AwsAsfActions.AWS_ASF_PASS,
                           context=context)
        elif state['Type'] == 'Task':
            ep.add_trigger(CloudEvent(upstream_relatives[state_name]),
                           condition=AwsAsfConditions.AWS_ASF_CONDITION,
                           action=AwsAsfActions.AWS_ASF_TASK,
                           context=context)
        elif state['Type'] == 'Choice':
            for choice in state['Choices']:
                context['condition'] = choice.copy()
                ep.add_trigger(CloudEvent(upstream_relatives[choice['Next']]),
                               condition=AwsAsfConditions.AWS_ASF_CONDITION,
                               action=AwsAsfActions.AWS_ASF_CHOICE,
                               context=context)
        elif state['Type'] == 'Parallel':
            for branch in state['Branches']:
                state_machine(branch, iterator)
        elif state['Type'] == 'Wait':
            # TODO Ask Pedro about timeouts again
            pass
        elif state['Type'] == 'Map':
            # This won't work
            state_machine(state['Iterator'], True)
        elif state['Type'] == 'Succeed':
            pass
        elif state['Type'] == 'Fail':
            pass

    ep.add_trigger([CloudEvent(final_state) for final_state in final_states],
                   condition=AwsAsfConditions.AWS_ASF_CONDITION,
                   action=AwsAsfActions.TERMINATE)


puta = """{
  "Comment": "A Hello World example of the Amazon States Language using Pass states",
  "StartAt": "Hello",
  "States": {
    "Hello": {
      "Type": "Pass",
      "Result": "Hello",
      "Next": "World"
    },
    "World": {
      "Type": "Pass",
      "Result": "World",
      "End": true
    }
  }
}"""

import json
asf2triggers(json.loads(puta))
