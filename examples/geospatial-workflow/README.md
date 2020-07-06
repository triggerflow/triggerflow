# Geospatial Workflow Example

To demonstrate the feasibility of event-based orchestration with Triggerflow for serverless scientific workflows, we 
implemented this geospatial workflow that runs over IBM Cloud Functions and IBM Cloud Object Storage.

The objective of the workflow is to compute the evapotranspiration and water consumption from crops and vegetation of
plots of land.

![evapotranspiration](https://user-images.githubusercontent.com/33722759/86574426-1f46c800-bf76-11ea-96f6-316a20441ca4.png)

First, each plot of land is partitioned to increase parallelism. For each partition, we interpolate the solar radiance,
temperature, humidity, and wind intensity. Then, the partitioned chunks are merged, and for every merged plot of land,
we calculate the evapotranspiration using the Penman Monteith formula and the data processed from the previous steps.

## Steps to execute the workflow

### Prerequisites

- The Triggerflow client installed and configured.
- The **DAGs** and **IBM Cloud Functions** section from the `triggerflow_config.yaml` configuration file filled. 
- The Triggerflow backend (trigger API, service, database and event broker) deployed.
- An [IBM Cloud account](https://cloud.ibm.com/docs/overview?topic=overview-quickstart_lite).
- [IBM Cloud CLI](https://cloud.ibm.com/docs/cli) installed and configured.
- An [IBM Cloud Functions namespace](https://cloud.ibm.com/docs/openwhisk?topic=openwhisk-namespaces) created.
- A [IBM Cloud Object Storage](https://www.ibm.com/cloud/object-storage) bucket created. 

### Instructions

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

8. Retrieve the results using the Triggerflow CLI. The result raster files can be downloaded from IBM COS.
```
$ triggerflow dag result $DAGRUN_ID
```



