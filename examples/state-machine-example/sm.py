import json

from triggerflow.statemachine import StateMachine, trigger_statemachine

runid = StateMachine.json('mixed.json')

trigger_statemachine(runid)
