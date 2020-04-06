# Triggerflow for KEDA

## Instructions

1. docker build -f keda/Dockerfile.controller -t jsampe/triggerflow-keda-controller .
2. docker build -f keda/Dockerfile.worker -t jsampe/triggerflow-keda-worker .
3. kubectl apply -f keda/clusterrole.yaml
4. kubectl apply -f keda/controller.yaml
