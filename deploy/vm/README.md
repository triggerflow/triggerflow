# Triggerflow local installation for developing and testing purposes

The following guide provides instructions for a local installation.

Please note that in order to execute the serverless orchestration examples, the functions will need to publish their
termination events to the event broker, so it **must be publicly addressable**.

## Installation

1. [Install Docker CE](https://docs.docker.com/engine/install/)


2. Install Triggeflow:

    ```
    git clone https://github.com/triggerflow/triggerflow
    cd triggerflow
    python3 setup.py develop
    ```

## Configuration

1. Create the configuration file for the API. Create a file named `config_map.ymal`, locate it into the `trigger-api\` and `deploy\vm\`
directories, and fill in the following configuration parameters:

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

2. Start the database and event broker backends. Using Redis for both in a docker container is recommended.
```
$ docker run --rm -d --name triggerflow-redis redis:6.0-rc4-alpine --requirepass myredispassword
```

3. Create a client user and password. Execute the script `add_user.yaml` located in the `trigger-api` directory:
```
$ cd triggerflow/trigger-api
$ python3 add_user.py config_map.yaml admin admin
```

## Usage

1. Start the Triggerflow API.
```
$ cd triggerflow/trigger-api
$ python3 api/api.py
```

2. Start the Triggerflow Controller.
```
$ cd triggerflow/deploy/vm
$ python3 controller.py
```

3. Verify the deployment by running the [hello_world.py](../../examples/hello_world.py) example.

