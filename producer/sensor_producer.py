import json
import os
import time

import requests
from kafka import KafkaProducer


def main():
    base_url = os.getenv("API_BASE_URL", "http://traffic-api:8000")
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "trafic.raw_sensor")
    poll_interval = float(os.getenv("POLL_INTERVAL_MS", "1000")) / 1000

    routes = requests.get(f"{base_url}/roads", timeout=10).json()
    if not routes:
        raise RuntimeError("No routes returned from traffic API")

    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    print(f"Starting sensor producer: {len(routes)} routes, interval={poll_interval}s")

    tick = 0
    while True:
        route = routes[tick % len(routes)]
        params = {
            "road_name": route["road_name"],
            "district": route["district"],
            "city": route["city"],
        }
        try:
            traffic = requests.get(f"{base_url}/events/traffic", params=params, timeout=10)
            traffic.raise_for_status()

            vehicles = requests.get(f"{base_url}/events/vehicles", params=params, timeout=10)
            vehicles.raise_for_status()

            event = {**traffic.json(), **vehicles.json()}
            producer.send(kafka_topic, value=event)
            producer.flush()
            print(f"[{tick}] sensor sent: {route['road_name']}")
        except Exception as e:
            print(f"[{tick}] error: {e}")

        tick += 1
        time.sleep(poll_interval)


if __name__ == "__main__":
    main()
