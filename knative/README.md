# Triggerflow for Knative
## Instructions:

1.  kubectl apply -f https://github.com/knative/eventing-contrib/releases/download/v0.12.3/kafka-source.yaml
2.  kubectl apply -f https://github.com/knative/eventing-contrib/releases/download/v0.12.3/event-display.yaml
3.  docker build -f knative/Dockerfile.controller -t jsampe/triggerflow-knative-controller .
4.  docker build -f knative/Dockerfile.worker -t jsampe/triggerflow-knative-worker .
5.  kubectl apply -f knative/controller.yaml