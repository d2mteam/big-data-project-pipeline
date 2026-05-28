import json
import os
from datetime import datetime, timedelta

from fastapi import FastAPI
from kafka import KafkaProducer


app = FastAPI()

kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
kafka_topic = os.getenv("KAFKA_TOPIC", "trafic.raw_sensor")
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
)


def make_event(
    timestamp,
    road_name="DEMO CEP API",
    district="Demo",
    city="Ha Noi",
    avg_speed=8.0,
    vehicle_count=420,
    accident_count=0,
):
    hour = timestamp.hour
    return {
        "timestamp": timestamp.isoformat(timespec="seconds"),
        "road_name": road_name,
        "district": district,
        "city": city,
        "latitude": 21.005,
        "longitude": 105.85,
        "vehicle_count": int(vehicle_count),
        "avg_speed": round(float(avg_speed), 2),
        "min_speed": round(float(avg_speed) * 0.5, 2),
        "max_speed": round(float(avg_speed) * 1.6, 2),
        "avg_delay_minutes": round(max(0, (25 - float(avg_speed)) * 0.8), 2),
        "truck_count": int(vehicle_count * 0.02),
        "bus_count": int(vehicle_count * 0.03),
        "motorbike_count": int(vehicle_count * 0.8),
        "car_count": int(vehicle_count * 0.1),
        "taxi_count": int(vehicle_count * 0.05),
        "truck_ratio": 0.02,
        "bus_ratio": 0.03,
        "motorbike_ratio": 0.8,
        "car_ratio": 0.1,
        "taxi_ratio": 0.05,
        "accident_count": int(accident_count),
        "temperature_celsius": 28,
        "humidity_percentage": 70,
        "weather_condition": "Clear",
        "is_rain": 0,
        "hour": hour,
        "day_of_week": timestamp.weekday(),
        "is_weekend": 1 if timestamp.weekday() >= 5 else 0,
        "is_peak_hour": 1 if (7 <= hour <= 9 or 16 <= hour <= 19) else 0,
    }


def send_events(events):
    for event in events:
        producer.send(kafka_topic, value=event)
    producer.flush()
    return {
        "topic": kafka_topic,
        "sent": len(events),
        "first_timestamp": events[0]["timestamp"] if events else None,
        "last_timestamp": events[-1]["timestamp"] if events else None,
    }


@app.get("/health")
def health():
    return {"status": "ok", "topic": kafka_topic}


@app.post("/trigger/congestion")
def trigger_congestion(
    road_name: str = "DEMO CEP API",
    district: str = "Demo",
    city: str = "Ha Noi",
    avg_speed: float = 7.0,
):
    now = datetime.utcnow().replace(second=0, microsecond=0)
    # raw_to_enriched emits one enriched event after its first 3 raw events,
    # then one per new raw event. Send 5 raw events to produce 3 enriched
    # low-speed events for the CEP job.
    events = [
        make_event(
            now + timedelta(minutes=15 * i),
            road_name=road_name,
            district=district,
            city=city,
            avg_speed=avg_speed,
            vehicle_count=420 + i * 30,
            accident_count=0,
        )
        for i in range(5)
    ]
    result = send_events(events)
    result["expected_alert_topic"] = "trafic.congestion_alerts"
    result["rule"] = "5 raw events -> 3 enriched events avg_speed < 10 km/h"
    return result


@app.post("/trigger/accident")
def trigger_accident(
    road_name: str = "DEMO ACCIDENT API",
    district: str = "Demo",
    city: str = "Ha Noi",
    accident_count: int = 2,
):
    now = datetime.utcnow().replace(second=0, microsecond=0)
    events = [
        make_event(
            now,
            road_name=road_name,
            district=district,
            city=city,
            avg_speed=28,
            vehicle_count=260,
            accident_count=0,
        ),
        make_event(
            now + timedelta(minutes=15),
            road_name=road_name,
            district=district,
            city=city,
            avg_speed=24,
            vehicle_count=300,
            accident_count=0,
        ),
        make_event(
            now + timedelta(minutes=30),
            road_name=road_name,
            district=district,
            city=city,
            avg_speed=9,
            vehicle_count=480,
            accident_count=accident_count,
        ),
    ]
    result = send_events(events)
    result["expected_alert_topic"] = "trafic.accident_anomaly_alerts"
    result["rule"] = "accident_count spikes after clean history"
    return result


@app.post("/trigger/both")
def trigger_both():
    return {
        "congestion": trigger_congestion(),
        "accident": trigger_accident(),
    }
