#!/usr/bin/env bash
set -euo pipefail

REGISTRY="${REGISTRY:-localhost:5000}"

curl -s "http://${REGISTRY}/v2/_catalog"
echo

