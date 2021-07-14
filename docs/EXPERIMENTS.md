# Ingestion test

The objective of this experiment is to evaluate the performance of a single Triggerflow instance to determine
the maximum throughput (events/second) that a worker can process.

The experiment simulates an intense workflow and it performs a massive fork-join operation of 200 parallel tasks.
Each task will produce 1000 events, so in total, the worker will process 200000 events. The worker must join and
aggregate the events correctly without losing any event in the process.

In addition, the tests include a script for consuming the same quantity of events but without doing any fork-join
operation, so the throughput can be calculated as a baseline to compare with the throughput that Triggerflow produces.

### Prerequisites

- The [Triggerflow Client](CLIENT_INSTALL.md) installed.
- A Triggerflow deployment up and running.
- An event broker instance available (Kafka, Redis, RabbitMQ...).
- A trigger storage instance available (Redis, CouchDB...).


### Instructions for replication

1. Create the workspace and trigger using [this script](/tests/ingestion-test/setup_kafka.py) for kafka, or
[this one](/tests/ingestion-test/setup_redis.py) for redis.
```
    $ python3 /tests/ingestion-test/setup_kafka.py
```
2. Then, produce all the events using the `produce_*.py` function from the same script. To have better performance
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
autoscaling policies (like with Kubernetes + KEDA or Knative Eventing), the resources are allocated only when it is
needed, resulting in a serverless and pay-per-use execution model.

The experiment consists of deploying a bunch of workspaces and producing events that activate the triggers from those
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
Find complete instrucions and examples [here](https://github.com/triggerflow/pywren-ibm-cloud_tf-patch).

# Fault tolerance scientific workflow

### Prerequisites

- The Triggerflow client installed and configured.
- The **DAGs** and **IBM Cloud Functions** section from the `triggerflow_config.yaml` configuration file filled. 
- The Triggerflow backend (trigger API, service, database and event broker) deployed.
- An [IBM Cloud account](https://cloud.ibm.com/docs/overview?topic=overview-quickstart_lite).
- [IBM Cloud CLI](https://cloud.ibm.com/docs/cli) installed and configured.
- An [IBM Cloud Functions namespace](https://cloud.ibm.com/docs/openwhisk?topic=openwhisk-namespaces) created.
- A [IBM Cloud Object Storage](https://www.ibm.com/cloud/object-storage) bucket created. 

### Instructions for replication

1. Set up the environment variables.
The following environment variables must be set:
- `BUCKET`: The IBM COS bucket for storing the input, intermediante and result data.
- `AWS_ACCESS_KEY_ID`: The HMAC access key id COS service credential.  
- `AWS_SECRET_ACCESS_KEY`: The HMAC secret access key COS service credential.
To create an HMAC credentials for your bucket please [follow these instructions](https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-uhc-hmac-credentials-main).

2. Download the MDT files for free from [here](http://centrodedescargas.cnig.es/CentroDescargas/buscadorCatalogo.do?codFamilia=MDT05#)
and place them in a directory named `mdt` in this path.

3. Execute the `upload_mdt_tiles.sh` script. This script uploads the previously downloaded MDT files to your IBM COS bucket.

4. Upload the `shapefile.zip` file needed by some tasks of the workflow. To upload the file to your IBM COS bucket,
execute the following commands:
```
$ wget https://aitor-data.s3.amazonaws.com/public/shapefile.zip -O /tmp/shapefile.zip
$ ibmcloud cos upload --bucket $BUCKET --key shapefile.zip --file /tmp/shapefile.zip
$ rm /tmp/shapefile.zip
```

5. Deploy the workflow's serverless functions. First, create a package that will contain all the workflow's functions:
```
$ ibmcloud wsk package create geospatial-dag
```
Then, execute the `create_functions.sh` script. This script creates the functions located in the `functions` directory. 

6. Build the DAG. Run the `dag.py` file to compile and save the DAG.
```
$ python3 dag.py
```

7. Once the DAG is built, run it by using the Triggerflow CLI:
```
$ triggerflow dag run water_consumption
```

8. During the execution of the workflow, you can stop the worker that is processing the workflow to simulate a 
*system failure*. If the backend in deployed on Kubernetes using KEDA or Knative, you can kill the worker pod, and the
system will automatically provision the pod again. If the backend is deployed on Docker or locally using processes,
you will have to manually provision the worker container. In both cases, the worker should recover the state of the
workflow by retrieving the triggers' context from the DB and uncommitted events from the event broker.

# Montage Workflow

### Prerequisites

- The Triggerflow client installed and configured.
- The **DAGs** and **IBM Cloud Functions** section from the `triggerflow_config.yaml` configuration file filled. 
- The Triggerflow backend (trigger API, service, database and event broker) deployed.
- An [AWS](https://aws.amazon.com/) account.

### Instructions

1. Create a AWS S3 bucket (for example, name it `montage`).

2. Create a lambda layer containing the necessary dependencies for the lambda functions:
    1. Run the script `create_layer.sh`. This will create a zip file containing the dependencies. Upload this zip as a Layer.

3. Create subnets for Lambda in every availability zone.
    1. Create a subnet in every availability zone, using for example, the default VPC.
    2. Create a NAT gateway.
    3. Create a route table.
    4. Add the NAT gateway to the route table as the default gateway.
    5. Assign this route table to every subnet created before.

5. Create a EFS file system.
    1. Create a EFS in the desired VPC.
    2. Attach the EFS to the subnets created before.
    3. Create an access point for the EFS to use with lambda. Note that the mount point must be `/mnt/lambda`

6. Compile the Montage binaries.
    1. Go to [Montage Downloads](http://montage.ipac.caltech.edu/docs/download2.html) and download the latest version source.
    2. You need to compile the binaries in a machine compatible with Lambda. For example, you can compile it with a EC2 instance running Amazon Linux 2.
    3. Copy the compiled binaries (`/bin`) into the `/bin` [directory](../examples/montage-workflow/lambdas/bin).

6. Create the lambda functions.
    1. Run the `create_lambda.sh` script. This will create three zip files, each one is a different lambda function.
    2. When creating the lambda function, assign it a role that has access to S3.
    3. Attach the Lambdas to the desired VPC within the previously created subnets.
    4. Attach the EFS volume created previously. Note that the mount point must be `/mnt/lambda`
    5. Add the following ENV vars to your lambdas:
        - BUCKET: The name of the bucket created before.
        - REGION: The region where the bucket is located.
        - TARGET_DIR: The PWD of the lambdas, in this case set it as `/mnt/lambda`
   
7. Upload the uncompressed content of the `data.zip` archive to the bucket root.

8. Replace the `Resource` attribute of the `montage.json` state machine with your own lambda ARNs.

9. Execute `run.py` 
