import asyncio
import json
import os
import re
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path

from aiokafka import AIOKafkaProducer

DATA_FILE = os.getenv("TRAFFIC_DATA_FILE", "/data/hanoi_traffic_train_data.json")
BASE_TIME = datetime.fromisoformat(os.getenv("BASE_TIME", "2024-01-01T00:00:00"))
STEP_MINUTES = int(os.getenv("STEP_MINUTES", "15"))

EMIT_FIELDS = {
    "timestamp", "road_name", "district", "city", "latitude", "longitude",
    "vehicle_count", "avg_speed", "min_speed", "max_speed", "avg_delay_minutes",
    "hour", "day_of_week", "is_weekend", "is_peak_hour",
    "truck_count", "bus_count", "motorbike_count", "car_count", "taxi_count",
    "truck_ratio", "bus_ratio", "motorbike_ratio", "car_ratio", "taxi_ratio",
}


def _slug(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value)
    without_marks = "".join(c for c in normalized if unicodedata.category(c) != "Mn")
    without_marks = without_marks.replace("đ", "d").replace("Đ", "D")
    return re.sub(r"[^a-z0-9]+", "-", without_marks.lower()).strip("-")


def load_data():
    records = json.loads(Path(DATA_FILE).read_text(encoding="utf-8"))
    by_route: dict[tuple, list] = {}
    for rec in records:
        key = (_slug(rec["road_name"]), _slug(rec["district"]), _slug(rec["city"]))
        by_route.setdefault(key, []).append(rec)
    for recs in by_route.values():
        recs.sort(key=lambda r: r.get("timestamp", ""))
    routes = [{"road_name": k[0], "district": k[1], "city": k[2]} for k in by_route]
    return routes, by_route


def next_event(route: dict, by_route: dict, cursors: dict) -> dict:
    key = (route["road_name"], route["district"], route["city"])
    idx = cursors.get(key, 0)
    cursors[key] = idx + 1
    rec = dict(by_route[key][idx % len(by_route[key])])
    ts = BASE_TIME + timedelta(minutes=STEP_MINUTES * idx)
    rec["timestamp"] = ts.strftime("%Y-%m-%dT%H:%M:%S")
    rec["hour"] = ts.hour
    rec["day_of_week"] = ts.weekday()
    rec["is_weekend"] = int(ts.weekday() >= 5)
    rec["is_peak_hour"] = int(7 <= ts.hour <= 9 or 16 <= ts.hour <= 19)
    return {k: v for k, v in rec.items() if k in EMIT_FIELDS}


async def main():
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "trafic.raw_sensor")
    batch_interval = float(os.getenv("POLL_INTERVAL_MS", "1000")) / 1000
    producer_id = int(os.getenv("PRODUCER_ID", "0"))
    producer_count = int(os.getenv("PRODUCER_COUNT", "1"))

    routes, by_route = load_data()
    my_routes = [r for i, r in enumerate(routes) if i % producer_count == producer_id]
    cursors: dict[tuple, int] = {}

    kafka = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
    await kafka.start()
    print(f"[producer-{producer_id}/{producer_count}] {len(my_routes)}/{len(routes)} routes, interval={batch_interval}s")

    try:
        tick = 0
        while True:
            t0 = asyncio.get_event_loop().time()
            await asyncio.gather(*[
                kafka.send(
                    kafka_topic,
                    json.dumps(next_event(r, by_route, cursors), ensure_ascii=False).encode()
                )
                for r in my_routes
            ])
            await kafka.flush()
            elapsed = asyncio.get_event_loop().time() - t0
            print(f"[{tick}] sent {len(my_routes)} in {elapsed*1000:.1f}ms")
            tick += 1
            await asyncio.sleep(max(0.0, batch_interval - elapsed))
    finally:
        await kafka.stop()


if __name__ == "__main__":
    asyncio.run(main())
