# Event processor for Knative
## Instructions:

1.  kubectl apply -f https://github.com/knative/eventing-contrib/releases/download/v0.12.3/kafka-source.yaml
2.  kubectl apply -f https://github.com/knative/eventing-contrib/releases/download/v0.12.3/event-display.yaml
3.  kubectl apply -f knative_service/kafka-event-source.yaml
4.  kubectl apply -f knative_service/event-processor.yaml
5.  docker build -f knative_service/Dockerfile -t jsampe/knative-event-processor .