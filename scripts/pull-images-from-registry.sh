#!/usr/bin/env bash
# Chạy trên máy 2 — pull images từ registry máy 1 và retag về tên local
# Usage: ./pull-images-from-registry.sh 192.168.2.108
set -euo pipefail

REGISTRY="${1:?Usage: $0 <registry_ip>  (vd: 192.168.2.108)}"
REGISTRY="$REGISTRY:5000"

G='\033[0;32m'; B='\033[0;34m'; N='\033[0m'
info() { echo -e "${B}▶${N} $*"; }
ok()   { echo -e "${G}✓${N} $*"; }

# registry_name → local_tag
declare -A IMAGES=(
  ["traffic-api:latest"]="big-data-project-traffic-api:latest"
  ["producer:latest"]="big-data-project-producer:latest"
  ["flink-py:1.19.1"]="big-data-project-flink-py:1.19.1"
  ["airflow:latest"]="big-data-project-airflow:latest"
  ["mlflow:latest"]="big-data-project-mlflow:latest"
  ["streamlit-alert:latest"]="big-data-project-streamlit-alert:latest"
  ["superset:latest"]="big-data-project-superset:latest"
)

info "Pull từ $REGISTRY ..."
for registry_name in "${!IMAGES[@]}"; do
  remote="$REGISTRY/$registry_name"
  local_tag="${IMAGES[$registry_name]}"
  docker pull "$remote"
  docker tag  "$remote" "$local_tag"
  ok "$remote → $local_tag"
done

echo ""
ok "Xong! Tất cả images đã sẵn sàng."
docker images | grep "big-data-project"
