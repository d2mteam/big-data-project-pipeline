import json
import os
import time

import requests
from kafka import KafkaProducer
from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.number_seq import NumberSequenceSource


def main():
    api_url = os.getenv("API_URL", "http://traffic-api:8000/events/next")
    roads_url = os.getenv("ROADS_URL", "http://traffic-api:8000/roads")
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "trafic.raw_events")
    poll_interval_ms = int(os.getenv("POLL_INTERVAL_MS", "1000"))

    routes = requests.get(roads_url, timeout=10).json()
    if not routes:
        raise RuntimeError("No routes returned from traffic API")

    producer = None

    def send_event(value):
        nonlocal producer

        if producer is None:
            producer = KafkaProducer(
                bootstrap_servers=kafka_bootstrap,
                value_serializer=lambda text: text.encode("utf-8"),
            )

        route = routes[int(value) % len(routes)]
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

        event_json = json.dumps(
            response.json(),
            ensure_ascii=False,
            separators=(",", ":"),
        )

        producer.send(kafka_topic, value=event_json)
        producer.flush()

        if poll_interval_ms > 0:
            time.sleep(poll_interval_ms / 1000)

        return event_json

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = NumberSequenceSource(0, 9_223_372_036_854_775_807)
    ticks = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "tick-source",
        Types.LONG(),
    )

    ticks.map(send_event, output_type=Types.STRING()).print()

    env.execute("api-json-to-kafka")


if __name__ == "__main__":
    main()
