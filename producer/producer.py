import json
import os
import time

import requests
from kafka import KafkaProducer


def main():
    api_url = os.getenv("API_URL", "http://traffic-api:8000/events/next")
    roads_url = os.getenv("ROADS_URL", "http://traffic-api:8000/roads")
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "trafic.raw_events")
    poll_interval = float(os.getenv("POLL_INTERVAL_MS", "1000")) / 1000

    routes = requests.get(roads_url, timeout=10).json()
    if not routes:
        raise RuntimeError("No routes returned from traffic API")

    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    print(f"Starting producer: {len(routes)} routes, interval={poll_interval}s")

    tick = 0
    while True:
        route = routes[tick % len(routes)]
        try:
            response = requests.get(
                api_url,
                params={
                    "road_name": route["road_name"],
                    "district": route["district"],
                    "city": route["city"],
                },
                timeout=10,
            )
            response.raise_for_status()
            event = response.json()
            producer.send(kafka_topic, value=event)
            producer.flush()
            print(f"[{tick}] sent: {route['road_name']}")
        except Exception as e:
            print(f"[{tick}] error: {e}")

        tick += 1
        time.sleep(poll_interval)


if __name__ == "__main__":
    main()
