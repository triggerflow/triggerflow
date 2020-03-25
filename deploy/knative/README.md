# Triggerflow for Knative > v0.13.0

## Instructions

1. kubectl apply -f https://github.com/knative/eventing-contrib/releases/download/v0.13.0/kafka-source.yaml
2. kubectl apply -f https://github.com/knative/eventing-contrib/releases/download/v0.13.0/event-display.yaml
3. docker build -f knative/Dockerfile.controller -t jsampe/triggerflow-knative-controller .
4. docker build -f knative/Dockerfile.worker -t jsampe/triggerflow-knative-worker .
5. kubectl apply -f knative/clusterrole.yaml
6. kubectl apply -f knative/controller.yaml
7. kubectl label namespace default knative-eventing-injection=enabled
