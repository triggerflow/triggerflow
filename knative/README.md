# Event processor for Knative
## Instructions:

1.  kubectl apply -f https://github.com/knative/eventing-contrib/releases/download/v0.12.3/kafka-source.yaml
2.  kubectl apply -f https://github.com/knative/eventing-contrib/releases/download/v0.12.3/event-display.yaml
3.  kubectl apply -f knative/kafka-event-source.yaml
4.  docker build -f knative/Dockerfile -t jsampe/triggerflow-controller .
5.  docker build -f knative/Dockerfile -t jsampe/triggerflow-worker .
6.  kubectl apply -f knative/controller.yaml