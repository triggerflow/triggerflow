# Ingestion test

The objective of this experiment is to evaluate the performance of a single Triggerflow instance to determine
the maximum throughput (events/second) that a worker can process.

The experiment simulates an intense workflow and it performs a massive fork-join operation of 200 parallel tasks.
Each task will produce 1000 events, so in total, the worker will process 200000 events. The worker must join and
aggregate the events correctly without losing any event in the process.

In addition, the tests includes a script for consuming the same quantity of events but without doing any fork-join
operation, so the throughput can be calculated as a baseline to compare with the throughput that Triggerflow produces.

### Prerequisites

- The Triggerflow Client module installed.
- A Triggerflow deployment up and running.
- An event broker instance available (Kafka, Redis, RabbitMQ...).
- A trigger storage instance available (Redis, CouchDB...).


### Instructions for replication

1. Create the workspace and trigger using [this script](/tests/ingestion-test/setup_kafka.py) for kafka, or
[this one](/tests/ingestion-test/setup_redis.py) for redis.
```
    $ python3 /tests/ingestion-test/setup_kafka.py
```
2. Then, produce all the events using the `produce_*.py` function from the same script. To have a better performance
and to avoid getting an incorrect result because of the maximum throughput a single publisher can produce, consider
producing the 200000 events from multiple nodes in parallel. For example, to produce the events from two separate
process, it would be:
```
    $ python3 /tests/ingestion-test/produce_kafka.py 1 2
```
```
    $ python3 /tests/ingestion-test/produce_kafka.py 2 2
```
Inside all the scripts there is a variable that determines the workspace name and event topic, so make sure the value is
the same for both scripts.

3. Then, start monitoring the worker container logs to calculate the time spent to aggregate all the events.

4. To execute the no-operation scripts, run:
```
    $ python3 /tests/ingestion-test/noop_kafka.py
```

# Autoscaling test

The objective of this test is to prove that, when Triggerflow is deployed on scalable platforms and using event-based
autoscaling polices (like with Kubernetes + KEDA or Knative Eventing), the resources are allocated only when it is
needed, resulting in a serverless and pay-per-use execution model.

The experiment consists on deploying a bunch of workspaces and producing events that activate the triggers from those
workspaces. Instead of producing all events at once, the events are generated in a pseudo-random way with pauses
in between, so that there are instances of time where a worker processing a workspace is not receiving any events and 
it will be scheduled for deprovisioning. After some time, there will be events produced that reactivate the previously
deprovisioned worker, so it will be provisioned again to continue processing events.

### Prerequisites

- Triggerflow deployed on KEDA or Knative Eventing.
- An event broker instance available (Kafka, Redis, RabbitMQ...).

### Instructions for replication

1. Deploy the triggers and workspace using this script. **TODO**
2. Produce the events using [this script](/tests/autoscaling_test.py). The script generates the events that activate the
triggers that are inside the workspaces previously deployed.
```
$ python3 test/autoscaling_test.py
```
3. Monitor the active workers. You should see how the workers are being provisioned and deprovisioned based on the
events that they are processing.
```
$ kubectl get pods -o wide -w
```

# DAGs overhead test

The objective of this experiment is to calculate the overhead produced when orchestrating a serverless workflow using
events and triggers for the DAGs interface.

### Prerequisites

- An IBM Cloud account.
- IBM Cloud CLI.
- A Cloud Foundry enabled IBM Cloud Functions namespace created. 
- The Triggerflow Client module installed.
- A Triggerflow deployment up and running.
- The [Triggerflow configuration file](/config/template.triggerflow_config.yaml) filled up for the DAGs and IBM Cloud
section and located in the `$HOME` directory.
- An event broker instance available (Kafka, Redis, RabbitMQ...).
- A trigger storage instance available (Redis, CouchDB...).


### Instructions for replication

1. Set the desired sequence length/map size parameter in the DAG definition file. That's the `sequence_length` variable 
in `/tests/dag-sequence/ibm_cf_sequence.py` variable for sequence workflow and `concurrency` in
`/tests/dag-map/ibm_cf_parallel.py` variable for parallel workflow.
2. Deploy the `sleep` function to IBM Cloud:
```
$ ibmcloud wsk package create triggerflow-tests
$ ibmcloud wsk action create triggerflow-tests/sleep --docker triggerflow/ibm_cloud_functions_runtime-v38:latest tests/dag-map/action.py
```

2. Build the DAGs:
```
$ triggerflow dag build tests/dag-sequence/ibm_cf_sequence.py
```
```
$ triggerflow dag build tests/dag-sequence/ibm_cf_map.py
```

3. Run the DAGs:
```
$ triggerflow dag run sequence
```
```
$ triggerflow dag run map
```

4. Monitor the worker's logs to determine the total execution time. Subtract from the total execution time the
time spent by the functions executing to get the overhead.

# Event sourcing overhead test

The objective of this experiment is to calculate the overhead produced when orchestrating a serverless workflow using
events and triggers for the workflow as code (event sourcing) interface. 

### Prerequisites

- An IBM Cloud account.
- IBM Cloud CLI.
- A Cloud Foundry enabled IBM Cloud Functions namespace created.
- IBM PyWren installed and configured. 
- The Triggerflow Client module installed.
- A Triggerflow deployment up and running.
- An event broker instance available (Kafka, Redis, RabbitMQ...).
- A trigger storage instance available (Redis, CouchDB...).


### Instructions for replication
**TODO**

# Fault tolerance scientific workflow

### Prerequisites

- An IBM Cloud account.
- IBM Cloud CLI.
- A Cloud Foundry enabled IBM Cloud Functions namespace created.
- A IBM Cloud Object Storage bucket created. 
- The Triggerflow Client module installed.
- The [Triggerflow configuration file](/config/template.triggerflow_config.yaml) filled up for the DAGs and IBM Cloud
section and located in the `$HOME` directory.
- A Triggerflow deployment up and running.
- An event broker instance available (Kafka, Redis, RabbitMQ...).
- A trigger storage instance available (Redis, CouchDB...).

### Instructions for replication
1. Download MDT files for free from [here](http://centrodedescargas.cnig.es/CentroDescargas/buscadorCatalogo.do?codFamilia=MDT05#)
and upload them using the `examples/upload_mdt_tiles.py` script.

2. Change the variable `keys` from the `examples/geospatial-workflow/dag.py` DAG file and replace them with the keys
of the tiles uploaded to IBM COS uploaded from the previous step.

3. Build the DAG:
```
$ triggerflow dag build examples/geospatial-workflow/dag.py
```

4. Run the DAG:
```
$ triggerflow dag run water_consumption
```

5. The results can be retrieved from COS using the `get_results.py` script.

6. In order to test the system's fault tolerance, you can stop the worker at any moment of the execution. When the
worker is restores (manually if deployed in standalone mode, or automatically when deployed with KEDA or Knative),
the worker will recover the workflow status from the trigger storage and the event broker, and will continue
with the workflow execution from the last checkpoint.