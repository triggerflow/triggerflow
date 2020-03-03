# Triggerflow for Kubernetes
## Instructions:

1.  docker build -f k8s/Dockerfile.controller -t jsampe/triggerflow-controller .
2.  docker build -f k8s/Dockerfile.worker -t jsampe/triggerflow-worker .
3.  kubectl apply -f k8s/controller.yaml