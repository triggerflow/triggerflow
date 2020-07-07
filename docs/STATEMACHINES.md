# State Machines Interface

Triggerflow provides an interface to orchestrate state machines using events and triggers.

The state machines are defined in Amazon States Language, so we can orchestrate existing state machine definitions proved
and running on Amazon Step Functions.

However, this implementation comes with some caveats:
- Only Lambda tasks are permitted since they can produce termination events through Lambda Destinations.
- `Wait` states are not supported, as Triggerflow currently lacks a time-based event source.

 [Here](/examples/state-machine-example) you can find an example of a toy state machine that combines `Choice`,
 `Parallel` and `Map` states to test the state machine interface with a complex workflow. 
 
 ![Screenshot_20200702_110036](https://user-images.githubusercontent.com/33722759/86339016-5c981680-bc53-11ea-88fb-53d1b8880da9.png)
 
 ### Instructions to execute the example State Machine
 
 1. Deploy the functions [provided](/examples/state-machine-example/functions) on Lambda.
 
 2. Replace the deployed functions ARN on the state machine definition file (sm.json).
 
 3. Have a Triggerflow instance up and running. You can find instructions [here](/docs/LOCAL_INSTALL.md).

 4. Run the following commands.
 
    To deploy the state machine execute:
    ```
    triggerflow statemachine deploy examples/state-machine-example/mixed.json
    ``` 
    This will give you a **run ID**.
    To trigger the state machine deployment execute:
    ```
    triggerflow statemachine trigger <RUN_ID>
    ```
    Where <RUN_ID> is the previously generated **run ID**.
 
 5. *(Optional)* Alternatively, you can deploy and run a state machine using the functions provided directly from a 
 script:
    ```python
    import json
    
    from triggerflow.statemachine import deploy_state_machine, trigger_statemachine
    
    with open('mixed.json', 'r') as sm_file:
        sm = json.loads(sm_file.read())
    
    runid = deploy_state_machine(sm)
    
    trigger_statemachine(runid)
    ``` 