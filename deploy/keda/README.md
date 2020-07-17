# Triggerflow Deployment on Kubernetes with KEDA

## Prerequisites
- KEDA installed on your kubernetes cluster. You can find more information about KEDA [here](https://keda.sh/docs/1.5/deploy/).
- A Trigger Storage (Redis, CouchDB...) and Event Broker (Kafka, Redis Streams, RabbitMQ...) deployed either on the same Kubernetes cluster or as independent services.


## Instructions

1. Create the Cluster Role with the necessary permissions so that the Controller can create Deployments and KEDA Scaled Objects.
```
kubectl apply -f clusterrole.yaml
```

2. Change the container's configuration env variables on `controller.yaml` (Storage backend and parameters).

3. Create the Controller deployment and service (the current service type is `NodePort`, you might want to change the service type).
```
kubectl apply -f controller.yaml
```

4. Now Triggerflow should be deployed. You can access the controller through the Trigger API using the deployment service created before. 
