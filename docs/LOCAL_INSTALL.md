# Triggerflow local installation for testing purposes

The following guide provides instructions for a local installation.

Please note that in order to execute the serverless orchestration examples, the functions will need to publish their
termination events to the event broker, so it **must be publicly accessible**.


1. Clone the repository and install the Triggerflow client. Using a virutal environment is recommended.
```
$ python3 -m venv venv
$ source venv/bin/activate
$ python setup.py install --user    
```

2. Start the database and event broker backends. Using Redis for both in a docker container is recommended.
```
$ docker run --rm -d --name triggerflow-redis redis:6.0-rc4-alpine --requirepass myredispassword
```

3. Create the configuration file for the API. Create a file named `config_map.ymal`, locate it into the `triggerflow-api`
directory and fill in the following configuration parameters:

```yaml
triggerflow_controller:
  endpoint: 127.0.0.1:5000
  token: token

trigger_storage:
  backend: redis
  parameters:
    host: <REDIS_HOST_IP>
    port: 6379
    password: myredispass
    db: 0
``` 

4. Create a client user and password. Execute the script `add_user.yaml` located in the `triggerflow-api` directory:
```
$ cd triggerflow-api
$ python add_user.py config_file.yaml admin admin
```

5. Start the Triggerflow API.
```
$ python api/api.py
```

6. Start the Triggerflow Controller.
```
$ python deploy/vm/controller.py
```

7. Configure the Triggerflow client credentials. Create a file named `triggerflow_config.yaml` located in the home directory,
and fill in the following parameters:
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

8. Now you should be able to execute the examples provided.

