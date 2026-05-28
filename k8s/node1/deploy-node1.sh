#!/usr/bin/env bash
set -euo pipefail

NODE_NAME="${1:-node1}"

if ! kubectl get node "$NODE_NAME" >/dev/null 2>&1; then
  echo "Node '$NODE_NAME' not found."
  echo "Available nodes:"
  kubectl get nodes
  exit 1
fi

kubectl label node "$NODE_NAME" traffic-node=node1 --overwrite
kubectl apply -f "$(dirname "$0")/node1.yaml"
kubectl -n traffic-infra rollout status deploy/registry --timeout=180s
kubectl -n traffic-infra rollout status deploy/minio --timeout=180s
kubectl -n traffic-infra rollout status deploy/iceberg-postgres --timeout=180s
kubectl -n traffic-infra rollout status deploy/iceberg-rest --timeout=180s
kubectl -n traffic-infra get pods,svc,pvc
