# Node 2 - Streaming

Deploy Kafka, Redpanda Console, Flink JobManager/TaskManager, Traffic API,
sensor/weather producers, va 3 Flink jobs:

- raw sensor/weather -> enriched events
- congestion CEP alerts
- accident anomaly alerts

Local kind flow:

```bash
k8s/node2/deploy-node2.sh demo-control-plane
```

Ports:

- Flink UI: `node-ip:30081`
- Redpanda Console: `node-ip:30082`

Kafka topics:

- `trafic.raw_sensor`
- `trafic.raw_weather`
- `trafic.enriched_events`
- `trafic.congestion_alerts`
- `trafic.accident_anomaly_alerts`

