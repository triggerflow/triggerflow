import json

from triggerflow.statemachine import deploy_state_machine, trigger_statemachine

with open('montage.json', 'r') as sm_file:
    sm = json.loads(sm_file.read())

# runid = deploy_state_machine(sm)

trigger_statemachine('sm_testt')
