# Workflow as code

Triggerflow provides an interface to orchestrate an imperative code workflow using events and triggers, with event sourcing techniques.

To run a **workflow as code** you have multiple options:

### Native workflow as code
Triggerflow provides a native interface to run a workflow as code by defining the functions to run in the triggers themselves.


### IBM-PyWren workflow as code
In this mode of execution the imperative code is defined using Pywren-like scripts. To make it running you need to install the IBM-Pywren library and apply the Triggerflow patch that contains the Triggerflow integration. Follow the [instructions to install IBM-PyWren and the Triggerflow patch](https://github.com/triggerflow/pywren-ibm-cloud_tf-patch) to enable this characteristic.

Once IBM-Pywren installed and the patch applied, you can define your scripts with all the functions of the workflow, and a main function called *orchestrator*, responsible to run the functions of the workflow, for example:

```python
def my_function(x):
    time.sleep(x)
    return x + 3

def main(args): # Orchestrator function
    pw = pywren.ibm_cf_executor(**args, log_level='INFO')
    res = 0
    pw.call_async(my_function, int(res))
    res = pw.get_result()
    pw.call_async(my_function, int(res))
    res = pw.get_result()
```

In the previous example, `my_function` is the unique function of the workflow, and according to the orchestrator, it is executed 2 times in a row: `my_function --> my_function`.  

Triggerflow for IBM-Pywren works as follows: 

1. The orchestrator function starts its execution for its very first time and executes the first `pw.call_async(my_function)` call. Immediately after the invocation, it creates a trigger in Triggerflow in order to awake itself when the invocation of `my_function` finishes its execution. Then, once the trigger is created, the orchestrator function shuts down its execution. 

2. When the first `my_function()` finishes its execution, it generates an event to Triggerflow. Then, thanks to the previous added trigger, the orchestrator function is awaken. In this 2nd execution, the orchestrator function will re-execute all the code. However, using an event source techinhe, it will see that the first `pw.call_async(my_function)` is done, so it will omit it and proceed to execute the 2nd `pw.call_async(my_function)`. It will again create a trigger in Triggerflow in order to awake itself when the invocation of `my_function` finishes its execution, eventually shuting down its execution.  

3. For each function invocation the same process is repeated until the orchestrator function finishes its execution.
