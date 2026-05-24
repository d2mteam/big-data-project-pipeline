import json
import os
import re
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any

from fastapi import FastAPI, HTTPException


DATA_FILE = Path(os.getenv("TRAFFIC_DATA_FILE", "/data/hanoi_traffic_train_data.json"))
BASE_TIME = datetime.fromisoformat(os.getenv("BASE_TIME", "2024-01-01T00:00:00"))
STEP_MINUTES = int(os.getenv("STEP_MINUTES", "15"))

app = FastAPI(title="Traffic Mock API")

_records: list[dict[str, Any]] = []
_records_by_route: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
_routes: list[dict[str, Any]] = []
_route_cursors: dict[tuple[str, str, str], int] = {}
_lock = Lock()

DERIVED_STREAM_OR_TRAINING_FIELDS = {
    "prev_avg_speed_1",
    "prev_avg_speed_2",
    "prev_avg_speed_3",
    "prev_vehicle_count_1",
    "prev_vehicle_count_2",
    "prev_vehicle_count_3",
    "prev_delay_1",
    "prev_delay_2",
    "prev_delay_3",
    "rolling_avg_speed_3",
    "rolling_vehicle_count_3",
    "rolling_delay_3",
    "target_15m",
    "target_30m",
    "target_60m",
}


def _slug(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value)
    without_marks = "".join(
        char for char in normalized if unicodedata.category(char) != "Mn"
    )
    without_marks = without_marks.replace("đ", "d").replace("Đ", "D")
    return re.sub(r"[^a-z0-9]+", "-", without_marks.lower()).strip("-")


def _route_key(road_name: str, district: str, city: str) -> tuple[str, str, str]:
    return (_slug(road_name), _slug(district), _slug(city))


def _load_records() -> list[dict[str, Any]]:
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"Traffic data file not found: {DATA_FILE}")

    with DATA_FILE.open("r", encoding="utf-8") as file:
        data = json.load(file)

    if not isinstance(data, list) or not data:
        raise ValueError("Traffic data file must contain a non-empty JSON array")

    return data


def _index_records(records: list[dict[str, Any]]) -> None:
    _records_by_route.clear()
    _routes.clear()
    _route_cursors.clear()

    route_seen: set[tuple[str, str, str]] = set()
    for record in records:
        key = _route_key(record["road_name"], record["district"], record["city"])
        _records_by_route.setdefault(key, []).append(record)

        if key not in route_seen:
            route_seen.add(key)
            _routes.append(
                {
                    "road_name": key[0],
                    "district": key[1],
                    "city": key[2],
                    "display_road_name": record["road_name"],
                    "display_district": record["district"],
                    "display_city": record["city"],
                }
            )

    for route_records in _records_by_route.values():
        route_records.sort(key=lambda item: item["timestamp"])


@app.on_event("startup")
def startup() -> None:
    global _records
    _records = _load_records()
    _index_records(_records)


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "mode": "replay_with_incrementing_timestamp",
        "records": len(_records),
        "routes": len(_routes),
        "step_minutes": STEP_MINUTES,
    }


@app.get("/roads")
def roads() -> list[dict[str, Any]]:
    return _routes


@app.get("/events/next")
def next_event(road_name: str, district: str, city: str) -> dict[str, Any]:
    if not _records:
        raise HTTPException(status_code=503, detail="Traffic data is not loaded")

    route_key = _route_key(road_name, district, city)
    route_records = _records_by_route.get(route_key)
    if not route_records:
        raise HTTPException(status_code=404, detail=f"Route not found: {route_key}")

    with _lock:
        route_index = _route_cursors.get(route_key, 0)
        _route_cursors[route_key] = route_index + 1

    record = dict(route_records[route_index % len(route_records)])
    for field in DERIVED_STREAM_OR_TRAINING_FIELDS:
        record.pop(field, None)

    timestamp = BASE_TIME + timedelta(minutes=STEP_MINUTES * route_index)
    record["timestamp"] = timestamp.strftime("%Y-%m-%dT%H:%M:%S")
    record["hour"] = timestamp.hour
    record["day_of_week"] = timestamp.weekday()
    record["is_weekend"] = int(timestamp.weekday() >= 5)
    record["is_peak_hour"] = int(7 <= timestamp.hour <= 9 or 16 <= timestamp.hour <= 19)

    return record
