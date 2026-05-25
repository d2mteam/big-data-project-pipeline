import json
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from kafka import KafkaConsumer, TopicPartition

from iceberg_utils import (
    ENRICHED_FQN,
    KAFKA_ENRICHED_FQN,
    ensure_iceberg_tables,
    run_trino,
)


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "trafic.enriched_events")
OFFSET_VARIABLE = os.getenv(
    "ICEBERG_INGEST_OFFSET_VARIABLE",
    "iceberg_ingest_offsets__trafic_enriched_events",
)
LOCAL_TIMEZONE = ZoneInfo(os.getenv("LOCAL_TIMEZONE", "Asia/Ho_Chi_Minh"))


BUSINESS_COLUMNS = [
    "road_name",
    "district",
    "city",
    "latitude",
    "longitude",
    "vehicle_count",
    "avg_speed",
    "min_speed",
    "max_speed",
    "avg_delay_minutes",
    "truck_count",
    "bus_count",
    "motorbike_count",
    "car_count",
    "taxi_count",
    "truck_ratio",
    "bus_ratio",
    "motorbike_ratio",
    "car_ratio",
    "taxi_ratio",
    "accident_count",
    "temperature_celsius",
    "humidity_percentage",
    "weather_condition",
    "is_rain",
    "hour",
    "day_of_week",
    "is_weekend",
    "is_peak_hour",
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
]


def get_kafka_offsets():
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
    )
    try:
        partitions = consumer.partitions_for_topic(KAFKA_TOPIC) or set()
        topic_partitions = [TopicPartition(KAFKA_TOPIC, item) for item in partitions]
        if not topic_partitions:
            return {}, {}
        starts = consumer.beginning_offsets(topic_partitions)
        ends = consumer.end_offsets(topic_partitions)
        start_offsets = {str(tp.partition): offset for tp, offset in starts.items()}
        end_offsets = {str(tp.partition): offset for tp, offset in ends.items()}
        return start_offsets, end_offsets
    finally:
        consumer.close()


def load_stored_offsets(defaults):
    raw_value = Variable.get(OFFSET_VARIABLE, default_var="")
    if not raw_value:
        return dict(defaults)
    try:
        loaded = json.loads(raw_value)
    except json.JSONDecodeError:
        return dict(defaults)
    offsets = dict(defaults)
    offsets.update({str(key): int(value) for key, value in loaded.items()})
    return offsets


def build_partition_filter(start_offsets, end_offsets):
    predicates = []
    for partition, end_offset in sorted(end_offsets.items(), key=lambda item: int(item[0])):
        start_offset = int(start_offsets.get(partition, 0))
        end_offset = int(end_offset)
        if end_offset > start_offset:
            predicates.append(
                "(_partition_id = "
                f"{int(partition)} AND _partition_offset >= {start_offset} "
                f"AND _partition_offset < {end_offset})"
            )
    return " OR ".join(predicates)


def ingest_kafka_to_iceberg():
    ensure_iceberg_tables()
    beginning_offsets, end_offsets = get_kafka_offsets()
    if not end_offsets:
        print(f"No partitions found for Kafka topic {KAFKA_TOPIC}.")
        return 0

    stored_offsets = load_stored_offsets(beginning_offsets)
    where_clause = build_partition_filter(stored_offsets, end_offsets)
    if not where_clause:
        print("No new Kafka offsets to ingest into Iceberg.")
        return 0

    target_columns = [
        '"timestamp"',
        "event_ts",
        "dt",
        "ingested_at",
        "source_topic",
        "source_partition",
        "source_offset",
        *BUSINESS_COLUMNS,
    ]
    select_columns = [
        '"timestamp"',
        'try_cast(replace("timestamp", \'T\', \' \') AS TIMESTAMP(3)) AS event_ts',
        'cast(try_cast(replace("timestamp", \'T\', \' \') AS TIMESTAMP(3)) AS DATE) AS dt',
        "cast(current_timestamp AS TIMESTAMP(3)) AS ingested_at",
        f"'{KAFKA_TOPIC}' AS source_topic",
        "cast(_partition_id AS INTEGER) AS source_partition",
        "cast(_partition_offset AS BIGINT) AS source_offset",
        *BUSINESS_COLUMNS,
    ]
    insert_values = [f"s.{column.strip(chr(34))}" for column in target_columns]
    insert_values[0] = 's."timestamp"'

    merge_sql = f"""
        MERGE INTO {ENRICHED_FQN} t
        USING (
            SELECT
                {", ".join(select_columns)}
            FROM {KAFKA_ENRICHED_FQN}
            WHERE {where_clause}
        ) s
        ON t.source_topic = s.source_topic
           AND t.source_partition = s.source_partition
           AND t.source_offset = s.source_offset
        WHEN NOT MATCHED THEN INSERT (
            {", ".join(target_columns)}
        )
        VALUES (
            {", ".join(insert_values)}
        )
    """
    run_trino(merge_sql)
    Variable.set(OFFSET_VARIABLE, json.dumps(end_offsets, sort_keys=True))
    inserted_until = ", ".join(
        f"p{partition}={offset}" for partition, offset in sorted(end_offsets.items())
    )
    print(f"Ingested Kafka offsets into Iceberg through {inserted_until}.")
    return sum(
        int(end_offsets[partition]) - int(stored_offsets.get(partition, 0))
        for partition in end_offsets
    )


with DAG(
    dag_id="kafka_enriched_to_iceberg",
    description="Ingest trafic.enriched_events from Kafka into Iceberg through Trino.",
    start_date=datetime(2026, 1, 1, tzinfo=LOCAL_TIMEZONE),
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=5),
    tags=["traffic", "kafka", "iceberg", "trino"],
) as dag:
    PythonOperator(
        task_id="ingest_kafka_to_iceberg",
        python_callable=ingest_kafka_to_iceberg,
    )
