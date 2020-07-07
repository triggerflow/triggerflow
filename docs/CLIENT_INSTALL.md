# Triggerflow Client installation and configuration

The following guide provides instructions for a the installation and configuration of the Triggerflow
client. 

1. Clone the repository and install the Triggerflow client. Using a virutal environment is recommended.
```
$ python3 -m venv venv
$ source venv/bin/activate
$ python setup.py install
```

2. Configure the Triggerflow client credentials. Create a file named `triggerflow_config.yaml` located in the home directory,
and fill in the following parameters:

A copy of the `triggerflow_config.yaml` configuration file can be found [here](/config/template.triggerflow_config.yaml).

```yaml
triggerflow:
  endpoint: 127.0.0.1:8080
  user: admin
  password: admin

dags:
  operators:
    ibm_cf:
      endpoint: <IBM_CF_ENDPOINT>
      namespace: <IBM_CF_NAMESPACE>
      api_key: <IBM_CF_API_KEY>
      event_source: redis
      redis:
        host: <REDIS_HOST_IP>
        port: 6379
        password: myredispassword
        db: 0
        
    aws_lambda:
        access_key_id: <AWS_ACCESS_KEY>
        secret_access_key: <AWS_SECRET_ACCESS_KEY>
        region_name: <LAMBDA_REGION_NAME>

statemachines:
  aws:
    access_key_id: <AWS_ACCESS_KEY>
    secret_access_key: <AWS_SECRET_ACCESS_KEY>
    region_name: <LAMBDA_REGION_NAME>
```
