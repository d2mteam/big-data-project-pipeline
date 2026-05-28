#!/usr/bin/env bash
# Push tất cả built images lên local registry để máy khác pull về
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
REGISTRY="${REGISTRY:-localhost:5000}"

G='\033[0;32m'; B='\033[0;34m'; N='\033[0m'
info() { echo -e "${B}▶${N} $*"; }
ok()   { echo -e "${G}✓${N} $*"; }

# local_tag → registry_name
declare -A IMAGES=(
  ["big-data-project-traffic-api:latest"]="traffic-api:latest"
  ["big-data-project-producer:latest"]="producer:latest"
  ["big-data-project-flink-py:1.19.1"]="flink-py:1.19.1"
  ["big-data-project-airflow:latest"]="airflow:latest"
  ["big-data-project-mlflow:latest"]="mlflow:latest"
  ["big-data-project-streamlit-alert:latest"]="streamlit-alert:latest"
  ["big-data-project-superset:latest"]="superset:latest"
)

info "Đảm bảo registry đang chạy..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" up -d registry

info "Build tất cả images..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" build

info "Push lên registry $REGISTRY ..."
for local_tag in "${!IMAGES[@]}"; do
  remote_tag="$REGISTRY/${IMAGES[$local_tag]}"
  docker tag "$local_tag" "$remote_tag"
  docker push "$remote_tag"
  ok "$local_tag → $remote_tag"
done

echo ""
ok "Xong! Máy khác pull về bằng:"
for local_tag in "${!IMAGES[@]}"; do
  echo "  docker pull $REGISTRY/${IMAGES[$local_tag]}"
done
