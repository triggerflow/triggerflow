# Triggerflow local installation for testing purposes

The following guide provides instructions for a local installation.

Please note that in order to execute the serverless orchestration examples, the functions will need to publish their
termination events to the event broker, so it **must be publicly addressable**.

1. Start the database and event broker backends. Using Redis for both in a docker container is recommended.
```
$ docker run --rm -d --name triggerflow-redis redis:6.0-rc4-alpine --requirepass myredispassword
```

2. Create the configuration file for the API. Create a file named `config_map.yaml`, locate it into the `triggerflow-api`
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

3. Create a client user and password. Execute the script `add_user.py` located in the `triggerflow-api` directory:
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

7. Now you should be able to execute the examples provided.

