#!/usr/bin/env bash
set -euo pipefail

NODE_CONTAINER="${1:-demo-control-plane}"

docker exec "$NODE_CONTAINER" mkdir -p /data/traffic
docker cp hanoi_traffic_train_data.json "$NODE_CONTAINER":/data/traffic/hanoi_traffic_train_data.json
docker exec "$NODE_CONTAINER" ls -lh /data/traffic/hanoi_traffic_train_data.json

