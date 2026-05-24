import math
import os
import re
import tempfile
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import boto3
import duckdb
import pandas as pd
import redis
from airflow import DAG
from airflow.operators.python import PythonOperator


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "traffic-lake")
MINIO_PREFIX = os.getenv("MINIO_PREFIX", "traffic/enriched_events")
LONGTERM_FEATURE_PREFIX = os.getenv(
    "LONGTERM_FEATURE_PREFIX", "traffic/longterm_features/staging"
)

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_TTL_SECONDS = int(os.getenv("REDIS_TTL_SECONDS", "90000"))

LOCAL_TIMEZONE = ZoneInfo(os.getenv("LOCAL_TIMEZONE", "Asia/Ho_Chi_Minh"))


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def list_parquet_keys(client, prefix):
    keys = []
    token = None
    while True:
        kwargs = {"Bucket": MINIO_BUCKET, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token

        response = client.list_objects_v2(**kwargs)
        keys.extend(
            item["Key"]
            for item in response.get("Contents", [])
            if item["Key"].endswith(".parquet")
        )

        if not response.get("IsTruncated"):
            return sorted(keys)
        token = response.get("NextContinuationToken")


def safe_name(value):
    return re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-") or "run"


def slug(value):
    text = str(value or "unknown").replace("Đ", "D").replace("đ", "d")
    normalized = unicodedata.normalize("NFKD", text)
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "_", ascii_value.lower()).strip("_") or "unknown"


def duckdb_list(paths):
    return "[" + ",".join("'" + path.replace("'", "''") + "'" for path in paths) + "]"


def query_parquet_features(**context):
    client = s3_client()
    source_keys = list_parquet_keys(client, MINIO_PREFIX)
    if not source_keys:
        print(f"No source parquet found under s3://{MINIO_BUCKET}/{MINIO_PREFIX}")
        return ""

    logical_time = context["logical_date"].astimezone(LOCAL_TIMEZONE)
    run_id = safe_name(context["run_id"])
    output_key = (
        f"{LONGTERM_FEATURE_PREFIX}/"
        f"dt={logical_time:%Y-%m-%d}/"
        f"hour={logical_time:%H}/"
        f"features_{run_id}.parquet"
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        local_paths = []
        for index, key in enumerate(source_keys):
            local_file = temp_path / f"source_{index:06d}.parquet"
            client.download_file(MINIO_BUCKET, key, str(local_file))
            local_paths.append(str(local_file))

        output_file = temp_path / "longterm_features.parquet"
        source_files = duckdb_list(local_paths)

        query = f"""
        COPY (
            WITH raw AS (
                SELECT DISTINCT
                    try_cast("timestamp" AS TIMESTAMP) AS event_ts,
                    road_name,
                    district,
                    city,
                    cast(hour AS INTEGER) AS event_hour,
                    cast(day_of_week AS INTEGER) AS event_dow,
                    cast(avg_speed AS DOUBLE) AS avg_speed,
                    cast(avg_delay_minutes AS DOUBLE) AS avg_delay_minutes
                FROM read_parquet({source_files})
                WHERE "timestamp" IS NOT NULL
                  AND road_name IS NOT NULL
                  AND district IS NOT NULL
                  AND city IS NOT NULL
            ),
            events AS (
                SELECT *
                FROM raw
                WHERE event_ts IS NOT NULL
                  AND event_hour BETWEEN 0 AND 23
                  AND event_dow BETWEEN 0 AND 6
            ),
            cutoff AS (
                SELECT max(event_ts) AS source_cutoff
                FROM events
            ),
            routes AS (
                SELECT DISTINCT road_name, district, city
                FROM events
            ),
            hours AS (
                SELECT range AS event_hour
                FROM range(0, 24)
            ),
            dows AS (
                SELECT range AS event_dow
                FROM range(0, 7)
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
                WHERE e.event_ts >= c.source_cutoff - INTERVAL 7 DAY
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
                WHERE e.event_ts >= c.source_cutoff - INTERVAL 28 DAY
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
                 AND cast(e.event_ts AS DATE) = cast(l.latest_date - INTERVAL 1 DAY AS DATE)
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
                strftime(c.source_cutoff, '%Y-%m-%dT%H:%M:%S') AS source_cutoff
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
            ORDER BY
                s.city,
                s.district,
                s.road_name,
                s.event_hour,
                s.event_dow
        ) TO '{str(output_file).replace("'", "''")}' (FORMAT PARQUET)
        """

        duckdb.connect(database=":memory:").execute(query)
        client.upload_file(str(output_file), MINIO_BUCKET, output_key)

    print(f"Wrote long-term features to s3://{MINIO_BUCKET}/{output_key}")
    return output_key


def clean_value(value):
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if pd.isna(value):
        return None
    return str(value)


def push_features_to_redis(**context):
    feature_key = context["ti"].xcom_pull(task_ids="query_parquet_features")
    if not feature_key:
        print("No feature parquet generated; skip Redis write.")
        return 0

    client = s3_client()
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

    with tempfile.NamedTemporaryFile(suffix=".parquet") as temp_file:
        client.download_file(MINIO_BUCKET, feature_key, temp_file.name)
        dataframe = pd.read_parquet(temp_file.name)

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
    description="Compute long-term traffic features from MinIO Parquet and push to Redis.",
    start_date=datetime(2026, 1, 1, tzinfo=LOCAL_TIMEZONE),
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    tags=["traffic", "features", "redis", "parquet"],
) as dag:
    query_task = PythonOperator(
        task_id="query_parquet_features",
        python_callable=query_parquet_features,
    )

    push_task = PythonOperator(
        task_id="push_features_to_redis",
        python_callable=push_features_to_redis,
    )

    query_task >> push_task
