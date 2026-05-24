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
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "trafic.enriched_events")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "airflow-minio-parquet-sink")
KAFKA_MAX_RECORDS = int(os.getenv("KAFKA_MAX_RECORDS", "5000"))
KAFKA_CONSUME_SECONDS = int(os.getenv("KAFKA_CONSUME_SECONDS", "45"))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "traffic-lake")
MINIO_PREFIX = os.getenv("MINIO_PREFIX", "traffic/enriched_events")
LOCAL_TIMEZONE = ZoneInfo(os.getenv("LOCAL_TIMEZONE", "Asia/Ho_Chi_Minh"))


def dump_enriched_events_to_minio(**context):
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    records = []
    started_at = time.time()
    try:
        while len(records) < KAFKA_MAX_RECORDS:
            if time.time() - started_at >= KAFKA_CONSUME_SECONDS:
                break

            polled = consumer.poll(timeout_ms=1000, max_records=500)
            if not polled:
                continue

            for messages in polled.values():
                for message in messages:
                    records.append(message.value)
                    if len(records) >= KAFKA_MAX_RECORDS:
                        break
                if len(records) >= KAFKA_MAX_RECORDS:
                    break

        if not records:
            print("No enriched events available; skip parquet write.")
            return

        logical_time = context["logical_date"].astimezone(LOCAL_TIMEZONE)
        object_key = (
            f"{MINIO_PREFIX}/"
            f"dt={logical_time:%Y-%m-%d}/"
            f"hour={logical_time:%H}/"
            f"enriched_events_{context['run_id'].replace(':', '-')}.parquet"
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
        print(f"Wrote {len(records)} records to s3://{MINIO_BUCKET}/{object_key}")
    finally:
        consumer.close()


with DAG(
    dag_id="kafka_enriched_to_minio_parquet",
    description="Dump trafic.enriched_events from Kafka to MinIO as Parquet.",
    start_date=datetime(2026, 1, 1, tzinfo=LOCAL_TIMEZONE),
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=5),
    tags=["traffic", "kafka", "minio", "parquet"],
) as dag:
    PythonOperator(
        task_id="dump_enriched_events_to_minio",
        python_callable=dump_enriched_events_to_minio,
    )
