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
    # asf_config = load_config_yaml('~/asf_config.yaml')

    # evt_src_type = asf_config['event_source']
    # evt_src_config = asf_config['event_sources'][evt_src_type]
    # evt_src_class = asf_config['event_sources'][evt_src_type]['class']
    # del evt_src_config['class']

    # mod = import_module('eventprocessor_client.sources')
    # evt_src = getattr(mod, '{}CloudEventSource'.format(evt_src_class))
    # event_source = evt_src(name=run_id,
    #                        topic=run_id,
    #                        **evt_src_config)

    ep = CloudEventProcessorClient(api_endpoint=ep_config['event_processor']['api_endpoint'],
                                   user=ep_config['event_processor']['user'],
                                   password=ep_config['event_processor']['password'],
                                   namespace=run_id,
                                   eventsource_name=run_id)

    # ep.create_namespace(run_id, event_source=event_source)

    state_machine(asf_json, '$init')


def state_machine(asf_json, init_event, iterator=False):
    global ep, sm
    this_sm = sm
    sm += 1
    upstream_relatives = {}
    final_states = []
    choices = {}

    for state_name, state in asf_json['States'].items():
        if 'End' in state and state['End']:
            final_states.append(state_name)
        elif 'Next' in state:
            upstream_relatives[state['Next']] = state_name
        elif state['Type'] == 'Choice':
            for choice in state['Choices']:
                upstream_relatives[choice['Next']] = state_name

    upstream_relatives[asf_json['StartAt']] = init_event

    for state_name, state in asf_json['States'].items():
        context = {'subject': state_name, 'iterator': iterator}
        context.update(state)
        if state_name in choices:
            context['Condition'] = choices[state_name].copy()
        if state['Type'] == 'Pass':
            ep.add_trigger(CloudEvent(upstream_relatives[state_name]),
                           condition=AwsAsfConditions.AWS_ASF_CONDITION,
                           action=AwsAsfActions.AWS_ASF_PASS,
                           context=context,
                           id=state_name)
        elif state['Type'] == 'Task':
            ep.add_trigger(CloudEvent(upstream_relatives[state_name]),
                           condition=AwsAsfConditions.AWS_ASF_CONDITION,
                           action=AwsAsfActions.AWS_ASF_TASK,
                           context=context)
        elif state['Type'] == 'Choice':
            # ep.add_trigger(CloudEvent(upstream_relatives[state_name]),
            #                condition=AwsAsfConditions.AWS_ASF_CONDITION,
            #                action=AwsAsfActions.AWS_ASF_PASS,
            #                context=context)
            choices = {}
            for choice in state['Choices']:
                choices[choice['Next']] = choice.copy()
        elif state['Type'] == 'Parallel':
            sub_state_machines = []
            for branch in state['Branches']:
                sm_id = state_machine(branch, upstream_relatives[state_name], iterator)
                sub_state_machines.append(sm_id)
            ep.add_trigger([CloudEvent(sub_state_machine_id) for sub_state_machine_id in sub_state_machines],
                           condition=AwsAsfConditions.AWS_ASF_CONDITION,
                           action=AwsAsfActions.AWS_ASF_PASS,
                           context=context)
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
                   action=AwsAsfActions.AWS_ASF_END_STATEMACHINE,
                   context={'subject': 'StateMachine{}'.format(this_sm)})

    return this_sm


my_statemachine = """
{
  "Comment": "Parallel Example.",
  "StartAt": "SetupParameters",
  "States": {
    "SetupParameters": {
      "Type": "Pass",
      "Result": 2,
      "ResultPath": "$.my_number",
      "Next": "Parallel"
    },
    "Parallel": {
      "Type": "Parallel",
      "End": true,
      "Branches": [
        {
          "StartAt": "Parallel1",
          "States": {
            "Parallel1": {
              "Type": "Pass",
              "End": true
            }
          }
        },
        {
          "StartAt": "Parallel2",
          "States": {
            "Parallel2": {
              "Type": "Pass",
              "Next": "Branching"
            },
            "Branching": {
              "Type": "Choice",
              "Choices": [
                {
                  "Variable": "$.my_number",
                  "NumericEquals": 1,
                  "Next": "Result1"
                },
                {
                  "Variable": "$.my_number",
                  "NumericEquals": 2,
                  "Next": "Result2"
                }
              ]
            },
            "Result1": {
              "Type": "Pass",
              "End": true
            },
            "Result2": {
              "Type": "Pass",
              "End": true
            }
          }
        }
      ]
    }
  }
}
"""

# my_statemachine = """
# {
#   "Comment": "A Hello World example of the Amazon States Language using Pass states",
#   "StartAt": "SetParameters",
#   "States": {
#     "SetParameters": {
#       "Type": "Pass",
#       "Result": 2,
#       "ResultPath": "$.my_number",
#       "Next": "Branching"
#     },
#     "Branching": {
#       "Type": "Choice",
#       "Choices": [
#         {
#           "Variable": "$.my_number",
#           "NumericLessThan": 0,
#           "Next": "Negative"
#         },
#         {
#           "Variable": "$.my_number",
#           "NumericGreaterThanEquals": 0,
#           "Next": "Positive"
#         }
#       ]
#     },
#     "Positive": {
#       "Type": "Pass",
#       "End": true
#     },
#     "Negative": {
#       "Type": "Pass",
#       "End": true
#     }
#   }
# }
# """

import json

asf2triggers(json.loads(my_statemachine))
