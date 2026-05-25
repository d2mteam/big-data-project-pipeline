import math
import os
import re
import unicodedata
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import redis
from airflow import DAG
from airflow.operators.python import PythonOperator

from iceberg_utils import (
    ENRICHED_FQN,
    LONGTERM_FQN,
    ensure_iceberg_tables,
    fetch_trino_dataframe,
    fetch_trino_value,
    run_trino,
)


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_TTL_SECONDS = int(os.getenv("REDIS_TTL_SECONDS", "90000"))
LOCAL_TIMEZONE = ZoneInfo(os.getenv("LOCAL_TIMEZONE", "Asia/Ho_Chi_Minh"))


def slug(value):
    text = str(value or "unknown").replace("Đ", "D").replace("đ", "d")
    normalized = unicodedata.normalize("NFKD", text)
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "_", ascii_value.lower()).strip("_") or "unknown"


def timestamp_literal(value):
    return "CAST('" + str(value).replace("T", " ").replace("'", "''") + "' AS TIMESTAMP(3))"


def query_iceberg_features():
    ensure_iceberg_tables()
    source_cutoff = fetch_trino_value(
        f"""
        SELECT max(event_ts)
        FROM {ENRICHED_FQN}
        WHERE event_ts IS NOT NULL
          AND road_name IS NOT NULL
          AND district IS NOT NULL
          AND city IS NOT NULL
        """
    )
    if source_cutoff is None:
        print("No Iceberg enriched events available; skip long-term feature compute.")
        return ""

    cutoff_sql = timestamp_literal(source_cutoff)
    run_trino(f"DELETE FROM {LONGTERM_FQN} WHERE source_cutoff = {cutoff_sql}")
    run_trino(
        f"""
        INSERT INTO {LONGTERM_FQN}
        WITH events AS (
            SELECT DISTINCT
                event_ts,
                road_name,
                district,
                city,
                cast(hour AS INTEGER) AS event_hour,
                cast(day_of_week AS INTEGER) AS event_dow,
                cast(avg_speed AS DOUBLE) AS avg_speed,
                cast(avg_delay_minutes AS DOUBLE) AS avg_delay_minutes
            FROM {ENRICHED_FQN}
            WHERE event_ts IS NOT NULL
              AND road_name IS NOT NULL
              AND district IS NOT NULL
              AND city IS NOT NULL
              AND hour BETWEEN 0 AND 23
              AND day_of_week BETWEEN 0 AND 6
        ),
        cutoff AS (
            SELECT {cutoff_sql} AS source_cutoff
        ),
        routes AS (
            SELECT DISTINCT road_name, district, city
            FROM events
        ),
        hours AS (
            SELECT value AS event_hour
            FROM UNNEST(sequence(0, 23)) AS t(value)
        ),
        dows AS (
            SELECT value AS event_dow
            FROM UNNEST(sequence(0, 6)) AS t(value)
        ),
        slots AS (
            SELECT
                r.road_name,
                r.district,
                r.city,
                h.event_hour,
                d.event_dow
            FROM routes r
            CROSS JOIN hours h
            CROSS JOIN dows d
        ),
        baseline_7d AS (
            SELECT
                e.road_name,
                e.district,
                e.city,
                avg(e.avg_speed) AS baseline_speed_7d
            FROM events e
            CROSS JOIN cutoff c
            WHERE e.event_ts >= date_add('day', -7, c.source_cutoff)
            GROUP BY e.road_name, e.district, e.city
        ),
        hour_dow_4w AS (
            SELECT
                e.road_name,
                e.district,
                e.city,
                e.event_hour,
                e.event_dow,
                avg(e.avg_speed) AS avg_speed_hour_dow_4w,
                avg(e.avg_delay_minutes) AS avg_delay_hour_dow_4w
            FROM events e
            CROSS JOIN cutoff c
            WHERE e.event_ts >= date_add('day', -28, c.source_cutoff)
            GROUP BY
                e.road_name,
                e.district,
                e.city,
                e.event_hour,
                e.event_dow
        ),
        latest_slot_date AS (
            SELECT
                e.road_name,
                e.district,
                e.city,
                e.event_hour,
                e.event_dow,
                max(cast(e.event_ts AS DATE)) AS latest_date
            FROM events e
            GROUP BY
                e.road_name,
                e.district,
                e.city,
                e.event_hour,
                e.event_dow
        ),
        yesterday AS (
            SELECT
                l.road_name,
                l.district,
                l.city,
                l.event_hour,
                l.event_dow,
                avg(e.avg_speed) AS avg_speed_same_hour_yesterday,
                avg(e.avg_delay_minutes) AS avg_delay_same_hour_yesterday
            FROM latest_slot_date l
            LEFT JOIN events e
              ON e.road_name = l.road_name
             AND e.district = l.district
             AND e.city = l.city
             AND e.event_hour = l.event_hour
             AND cast(e.event_ts AS DATE) = date_add('day', -1, l.latest_date)
            GROUP BY
                l.road_name,
                l.district,
                l.city,
                l.event_hour,
                l.event_dow
        )
        SELECT
            s.road_name,
            s.district,
            s.city,
            s.event_hour AS hour,
            s.event_dow AS day_of_week,
            b.baseline_speed_7d,
            hd.avg_speed_hour_dow_4w,
            hd.avg_delay_hour_dow_4w,
            y.avg_delay_same_hour_yesterday,
            y.avg_speed_same_hour_yesterday,
            c.source_cutoff,
            cast(c.source_cutoff AS DATE) AS dt,
            cast(current_timestamp AS TIMESTAMP(3)) AS updated_at
        FROM slots s
        CROSS JOIN cutoff c
        LEFT JOIN baseline_7d b
          ON b.road_name = s.road_name
         AND b.district = s.district
         AND b.city = s.city
        LEFT JOIN hour_dow_4w hd
          ON hd.road_name = s.road_name
         AND hd.district = s.district
         AND hd.city = s.city
         AND hd.event_hour = s.event_hour
         AND hd.event_dow = s.event_dow
        LEFT JOIN yesterday y
          ON y.road_name = s.road_name
         AND y.district = s.district
         AND y.city = s.city
         AND y.event_hour = s.event_hour
         AND y.event_dow = s.event_dow
        """
    )
    print(f"Wrote long-term features to {LONGTERM_FQN} at cutoff {source_cutoff}.")
    return str(source_cutoff)


def clean_value(value):
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if pd.isna(value):
        return None
    return str(value)


def push_features_to_redis(**context):
    source_cutoff = context["ti"].xcom_pull(task_ids="query_iceberg_features")
    if not source_cutoff:
        print("No feature rows generated; skip Redis write.")
        return 0

    dataframe = fetch_trino_dataframe(
        f"""
        SELECT
            road_name,
            district,
            city,
            hour,
            day_of_week,
            baseline_speed_7d,
            avg_speed_hour_dow_4w,
            avg_delay_hour_dow_4w,
            avg_delay_same_hour_yesterday,
            avg_speed_same_hour_yesterday,
            source_cutoff
        FROM {LONGTERM_FQN}
        WHERE source_cutoff = {timestamp_literal(source_cutoff)}
        ORDER BY city, district, road_name, hour, day_of_week
        """
    )
    if dataframe.empty:
        print("No long-term feature rows found for Redis write.")
        return 0

    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        socket_connect_timeout=5,
        socket_timeout=30,
        decode_responses=True,
    )

    updated_at = datetime.now(tz=LOCAL_TIMEZONE).isoformat(timespec="seconds")
    feature_columns = [
        "baseline_speed_7d",
        "avg_speed_hour_dow_4w",
        "avg_delay_hour_dow_4w",
        "avg_delay_same_hour_yesterday",
        "avg_speed_same_hour_yesterday",
    ]

    pipe = redis_client.pipeline(transaction=False)
    written = 0

    for row in dataframe.to_dict(orient="records"):
        redis_key = (
            "traffic:longterm:"
            f"{slug(row['city'])}:"
            f"{slug(row['district'])}:"
            f"{slug(row['road_name'])}:"
            f"h{int(row['hour']):02d}:"
            f"dow{int(row['day_of_week'])}"
        )

        mapping = {
            "updated_at": updated_at,
            "source_cutoff": str(row.get("source_cutoff") or ""),
        }
        for column in feature_columns:
            value = clean_value(row.get(column))
            if value is not None:
                mapping[column] = value

        pipe.hset(redis_key, mapping=mapping)
        pipe.expire(redis_key, REDIS_TTL_SECONDS)
        written += 1

        if written % 1000 == 0:
            pipe.execute()

    pipe.execute()
    print(f"Wrote {written} long-term feature hashes to Redis.")
    return written


with DAG(
    dag_id="compute_longterm_features",
    description="Compute long-term traffic features from Iceberg and push to Redis.",
    start_date=datetime(2026, 1, 1, tzinfo=LOCAL_TIMEZONE),
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    tags=["traffic", "features", "redis", "iceberg", "trino"],
) as dag:
    query_task = PythonOperator(
        task_id="query_iceberg_features",
        python_callable=query_iceberg_features,
    )

    push_task = PythonOperator(
        task_id="push_features_to_redis",
        python_callable=push_features_to_redis,
    )

    query_task >> push_task
