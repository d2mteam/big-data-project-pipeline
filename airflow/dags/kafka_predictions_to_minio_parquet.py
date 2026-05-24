import json
import os
import tempfile
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaConsumer


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
PREDICTIONS_TOPIC = os.getenv("PREDICTIONS_TOPIC", "trafic.predictions")
PREDICTIONS_GROUP_ID = os.getenv(
    "PREDICTIONS_GROUP_ID", "airflow-minio-predictions-sink"
)
PREDICTIONS_MAX_RECORDS = int(os.getenv("PREDICTIONS_MAX_RECORDS", "100000"))
PREDICTIONS_CONSUME_SECONDS = int(os.getenv("PREDICTIONS_CONSUME_SECONDS", "120"))
PREDICTIONS_IDLE_SECONDS = int(os.getenv("PREDICTIONS_IDLE_SECONDS", "10"))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "traffic-lake")
PREDICTIONS_MINIO_PREFIX = os.getenv(
    "PREDICTIONS_MINIO_PREFIX", "traffic/predictions"
)
LOCAL_TIMEZONE = ZoneInfo(os.getenv("LOCAL_TIMEZONE", "Asia/Ho_Chi_Minh"))


def flatten_prediction(record):
    predictions = record.get("predictions") or {}
    return {
        "road_name": record.get("road_name"),
        "district": record.get("district"),
        "city": record.get("city"),
        "timestamp": record.get("timestamp"),
        "target_15m": predictions.get("target_15m"),
        "target_30m": predictions.get("target_30m"),
        "target_60m": predictions.get("target_60m"),
    }


def dump_predictions_to_minio(**context):
    consumer = KafkaConsumer(
        PREDICTIONS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=PREDICTIONS_GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    records = []
    started_at = time.time()
    last_message_at = started_at

    try:
        while len(records) < PREDICTIONS_MAX_RECORDS:
            now = time.time()
            if now - started_at >= PREDICTIONS_CONSUME_SECONDS:
                break
            if records and now - last_message_at >= PREDICTIONS_IDLE_SECONDS:
                break

            polled = consumer.poll(timeout_ms=1000, max_records=1000)
            if not polled:
                continue

            last_message_at = time.time()
            for messages in polled.values():
                for message in messages:
                    records.append(flatten_prediction(message.value))
                    if len(records) >= PREDICTIONS_MAX_RECORDS:
                        break
                if len(records) >= PREDICTIONS_MAX_RECORDS:
                    break

        if not records:
            print("No predictions available; skip parquet write.")
            return

        logical_time = context["logical_date"].astimezone(LOCAL_TIMEZONE)
        run_id = context["run_id"].replace(":", "-")
        object_key = (
            f"{PREDICTIONS_MINIO_PREFIX}/"
            f"dt={logical_time:%Y-%m-%d}/"
            f"predictions_{run_id}.parquet"
        )

        dataframe = pd.DataFrame(records)
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            dataframe.to_parquet(tmp.name, index=False, engine="pyarrow")

            s3 = boto3.client(
                "s3",
                endpoint_url=MINIO_ENDPOINT,
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY,
            )
            s3.upload_file(tmp.name, MINIO_BUCKET, object_key)

        consumer.commit()
        print(f"Wrote {len(records)} predictions to s3://{MINIO_BUCKET}/{object_key}")
    finally:
        consumer.close()


with DAG(
    dag_id="kafka_predictions_to_minio_parquet",
    description="Dump daily trafic.predictions from Kafka to MinIO as Parquet.",
    start_date=datetime(2026, 1, 1, tzinfo=LOCAL_TIMEZONE),
    schedule="59 23 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    tags=["traffic", "predictions", "kafka", "minio", "parquet"],
) as dag:
    PythonOperator(
        task_id="dump_predictions_to_minio",
        python_callable=dump_predictions_to_minio,
    )
