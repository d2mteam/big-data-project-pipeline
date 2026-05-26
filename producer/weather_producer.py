import json
import os
import time

import requests
from kafka import KafkaProducer


def main():
    base_url = os.getenv("API_BASE_URL", "http://traffic-api:8000")
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "trafic.raw_weather")
    poll_interval = float(os.getenv("POLL_INTERVAL_MS", "10000")) / 1000

    routes = requests.get(f"{base_url}/roads", timeout=10).json()
    if not routes:
        raise RuntimeError("No routes returned from traffic API")

    seen: set[tuple[str, str]] = set()
    districts: list[dict] = []
    for r in routes:
        key = (r["district"], r["city"])
        if key not in seen:
            seen.add(key)
            districts.append({"district": r["district"], "city": r["city"]})

    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    print(f"Starting weather producer: {len(districts)} districts, interval={poll_interval}s")

    tick = 0
    while True:
        d = districts[tick % len(districts)]
        try:
            resp = requests.get(
                f"{base_url}/events/weather_district",
                params={"district": d["district"], "city": d["city"]},
                timeout=10,
            )
            resp.raise_for_status()
            producer.send(kafka_topic, value=resp.json())
            producer.flush()
            print(f"[{tick}] weather sent: {d['district']}, {d['city']}")
        except Exception as e:
            print(f"[{tick}] error: {e}")

        tick += 1
        time.sleep(poll_interval)


if __name__ == "__main__":
    main()
