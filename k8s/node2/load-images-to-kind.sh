#!/usr/bin/env bash
set -euo pipefail

CLUSTER="${1:-demo}"

kind load docker-image --name "$CLUSTER" big-data-project-flink-py:1.19.1
kind load docker-image --name "$CLUSTER" big-data-project-traffic-api:latest
kind load docker-image --name "$CLUSTER" big-data-project-traffic-sensor-producer:latest

