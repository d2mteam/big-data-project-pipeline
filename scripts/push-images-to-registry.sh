#!/usr/bin/env bash
set -euo pipefail

REGISTRY="${REGISTRY:-localhost:5000}"
PROJECT="${PROJECT:-traffic}"

echo "Starting local registry at ${REGISTRY}..."
docker compose up -d registry

echo "Building project images..."
docker build -t big-data-project-traffic-api ./traffic-api
docker build -t big-data-project-traffic-sensor-producer ./producer
docker build -t big-data-project-flink-py:1.19.1 ./flink
docker build -t big-data-project-airflow ./airflow
docker build -t big-data-project-mlflow ./mlflow
docker build -t big-data-project-streamlit-alert ./streamlit-alert
docker build -t big-data-project-superset ./superset

declare -A IMAGES=(
  ["traffic-api"]="big-data-project-traffic-api"
  ["producer"]="big-data-project-traffic-sensor-producer"
  ["flink-py"]="big-data-project-flink-py:1.19.1"
  ["airflow"]="big-data-project-airflow"
  ["mlflow"]="big-data-project-mlflow"
  ["streamlit-alert"]="big-data-project-streamlit-alert"
  ["superset"]="big-data-project-superset"
)

for name in "${!IMAGES[@]}"; do
  local_image="${IMAGES[$name]}"
  remote_image="${REGISTRY}/${PROJECT}/${name}:latest"
  echo "Tagging ${local_image} -> ${remote_image}"
  docker tag "${local_image}" "${remote_image}"
  echo "Pushing ${remote_image}"
  docker push "${remote_image}"
done

echo
echo "Pushed images:"
for name in "${!IMAGES[@]}"; do
  echo "  ${REGISTRY}/${PROJECT}/${name}:latest"
done
