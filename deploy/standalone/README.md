# Standalone Install (Docker Compose)

This file contains a guide to deploy all Triggerflow components at once as containerized services using Docker Compose.

**Please note** that this deployment uses the "monolith" version of Triggerflow using Linux processes to run workers, so
is not scalable nor serverless.

The `docker-compose.yml` file contains the following services:
- The Trigger API with a predefined user (username: `admin` password: `admin`) for testing.
- The Triggerflow Controller to process workspaces as Linux processes.
- A Redis 6.0.5 instance as Trigger storage and event broker (Redis Streams).
- An all-in-one Kafka instance ([`lensesio/fast-data-dev`](https://github.com/lensesio/fast-data-dev)) as event broker.
- A RabbitMQ instance as event broker.

All services are accessible at their respective ports using the machine IP:
- `8080`: Trigger API.
- `6379`: Redis storage/stream.
- `5672`: RabbitMQ broker.
- `15672`: RabbitMQ Management UI.
- `3030`: Lenses.io management UI.
- `9092`: Kafka bootstrap broker.

*Note*: Although you can access these services using `localhost`, those services will resolve `localhost` to their
respective local container. Use the machine's IP instead when creating event sources, etc.

## Installation guide

1. Install [docker compose](https://docs.docker.com/compose/install/).

2. Clone/download the Triggerflow repository.

3. Run the following command to start Triggerflow's services:
```
$ docker-compose -f deploy/standalone/docker-compose.yml up
```

4. Test that everything works: Run the [hello world example](../../examples/hello_world.py) to test the deployment.
Make sure to change the Triggerflow API endpoint and RabbitMQ source host IP on the script file (It should be the same
when using this deployment).




 



