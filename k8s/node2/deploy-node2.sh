#!/usr/bin/env bash
set -euo pipefail

NODE_NAME="${1:-demo-control-plane}"

kubectl label node "$NODE_NAME" traffic-node2=true --overwrite
"$(dirname "$0")/load-images-to-kind.sh" demo
"$(dirname "$0")/prepare-node-data.sh" "$NODE_NAME"
kubectl apply -f "$(dirname "$0")/node2.yaml"

kubectl -n traffic-streaming rollout status deploy/kafka --timeout=240s
kubectl -n traffic-streaming rollout status deploy/traffic-api --timeout=180s
kubectl -n traffic-streaming rollout status deploy/flink-jobmanager --timeout=180s
kubectl -n traffic-streaming rollout status deploy/flink-taskmanager --timeout=180s
kubectl -n traffic-streaming rollout status deploy/traffic-sensor-producer --timeout=180s
kubectl -n traffic-streaming rollout status deploy/traffic-weather-producer --timeout=180s
kubectl -n traffic-streaming get pods,svc
