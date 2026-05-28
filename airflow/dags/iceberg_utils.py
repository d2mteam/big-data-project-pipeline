import os

import pandas as pd
import trino


TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "airflow")
ICEBERG_CATALOG = os.getenv("TRINO_ICEBERG_CATALOG", "iceberg")
KAFKA_CATALOG = os.getenv("TRINO_KAFKA_CATALOG", "kafka")
ICEBERG_SCHEMA = os.getenv("ICEBERG_SCHEMA", "traffic")
ENRICHED_TABLE = os.getenv("ICEBERG_ENRICHED_TABLE", "enriched_events")
LONGTERM_TABLE = os.getenv("ICEBERG_LONGTERM_TABLE", "longterm_features")
TRAINING_TABLE = os.getenv("ICEBERG_TRAINING_TABLE", "training_kmeans_30m")


ENRICHED_FQN = f"{ICEBERG_CATALOG}.{ICEBERG_SCHEMA}.{ENRICHED_TABLE}"
LONGTERM_FQN = f"{ICEBERG_CATALOG}.{ICEBERG_SCHEMA}.{LONGTERM_TABLE}"
TRAINING_FQN = f"{ICEBERG_CATALOG}.{ICEBERG_SCHEMA}.{TRAINING_TABLE}"
KAFKA_ENRICHED_FQN = f"{KAFKA_CATALOG}.{ICEBERG_SCHEMA}.{ENRICHED_TABLE}"


def trino_connection():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=ICEBERG_CATALOG,
        schema=ICEBERG_SCHEMA,
    )


def run_trino(sql):
    with trino_connection() as connection:
        cursor = connection.cursor()
        cursor.execute(sql)
        try:
            return cursor.fetchall()
        except trino.exceptions.TrinoQueryError:
            raise
        except Exception:
            return []


def fetch_trino_dataframe(sql):
    with trino_connection() as connection:
        cursor = connection.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [item[0] for item in cursor.description or []]
    return pd.DataFrame(rows, columns=columns)


def fetch_trino_value(sql):
    rows = run_trino(sql)
    if not rows:
        return None
    return rows[0][0]


def sql_string(value):
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def ensure_iceberg_tables():
    run_trino(f"CREATE SCHEMA IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_SCHEMA}")

    run_trino(
        f"""
        CREATE TABLE IF NOT EXISTS {ENRICHED_FQN} (
            "timestamp" VARCHAR,
            event_ts TIMESTAMP(3),
            dt DATE,
            ingested_at TIMESTAMP(3),
            source_topic VARCHAR,
            source_partition INTEGER,
            source_offset BIGINT,
            road_name VARCHAR,
            district VARCHAR,
            city VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE,
            vehicle_count BIGINT,
            avg_speed DOUBLE,
            min_speed DOUBLE,
            max_speed DOUBLE,
            avg_delay_minutes DOUBLE,
            truck_count BIGINT,
            bus_count BIGINT,
            motorbike_count BIGINT,
            car_count BIGINT,
            taxi_count BIGINT,
            truck_ratio DOUBLE,
            bus_ratio DOUBLE,
            motorbike_ratio DOUBLE,
            car_ratio DOUBLE,
            taxi_ratio DOUBLE,
            accident_count BIGINT,
            temperature_celsius DOUBLE,
            humidity_percentage DOUBLE,
            weather_condition VARCHAR,
            is_rain INTEGER,
            hour INTEGER,
            day_of_week INTEGER,
            is_weekend INTEGER,
            is_peak_hour INTEGER,
            prev_avg_speed_1 DOUBLE,
            prev_avg_speed_2 DOUBLE,
            prev_avg_speed_3 DOUBLE,
            prev_vehicle_count_1 DOUBLE,
            prev_vehicle_count_2 DOUBLE,
            prev_vehicle_count_3 DOUBLE,
            prev_delay_1 DOUBLE,
            prev_delay_2 DOUBLE,
            prev_delay_3 DOUBLE,
            rolling_avg_speed_3 DOUBLE,
            rolling_vehicle_count_3 DOUBLE,
            rolling_delay_3 DOUBLE
        )
        WITH (
            format = 'PARQUET',
            format_version = 2,
            partitioning = ARRAY['dt']
        )
        """
    )

    run_trino(
        f"""
        CREATE TABLE IF NOT EXISTS {LONGTERM_FQN} (
            road_name VARCHAR,
            district VARCHAR,
            city VARCHAR,
            hour INTEGER,
            day_of_week INTEGER,
            baseline_speed_7d DOUBLE,
            avg_speed_hour_dow_4w DOUBLE,
            avg_delay_hour_dow_4w DOUBLE,
            avg_delay_same_hour_yesterday DOUBLE,
            avg_speed_same_hour_yesterday DOUBLE,
            source_cutoff TIMESTAMP(3),
            dt DATE,
            updated_at TIMESTAMP(3)
        )
        WITH (
            format = 'PARQUET',
            format_version = 2,
            partitioning = ARRAY['dt']
        )
        """
    )

    run_trino(
        f"""
        CREATE TABLE IF NOT EXISTS {TRAINING_FQN} (
            training_run_id VARCHAR,
            dt DATE,
            created_at TIMESTAMP(3),
            event_ts TIMESTAMP(3),
            road_name VARCHAR,
            district VARCHAR,
            city VARCHAR,
            vehicle_count DOUBLE,
            avg_speed DOUBLE,
            min_speed DOUBLE,
            max_speed DOUBLE,
            avg_delay_minutes DOUBLE,
            truck_ratio DOUBLE,
            bus_ratio DOUBLE,
            motorbike_ratio DOUBLE,
            car_ratio DOUBLE,
            taxi_ratio DOUBLE,
            accident_count DOUBLE,
            temperature_celsius DOUBLE,
            humidity_percentage DOUBLE,
            is_rain DOUBLE,
            hour INTEGER,
            day_of_week INTEGER,
            is_weekend DOUBLE,
            is_peak_hour DOUBLE,
            prev_avg_speed_1 DOUBLE,
            prev_avg_speed_2 DOUBLE,
            prev_avg_speed_3 DOUBLE,
            prev_vehicle_count_1 DOUBLE,
            prev_vehicle_count_2 DOUBLE,
            prev_vehicle_count_3 DOUBLE,
            prev_delay_1 DOUBLE,
            prev_delay_2 DOUBLE,
            prev_delay_3 DOUBLE,
            rolling_avg_speed_3 DOUBLE,
            rolling_vehicle_count_3 DOUBLE,
            rolling_delay_3 DOUBLE,
            baseline_speed_7d DOUBLE,
            avg_speed_hour_dow_4w DOUBLE,
            avg_delay_hour_dow_4w DOUBLE,
            avg_delay_same_hour_yesterday DOUBLE,
            avg_speed_same_hour_yesterday DOUBLE,
            feature_source_cutoff TIMESTAMP(3)
        )
        WITH (
            format = 'PARQUET',
            format_version = 2,
            partitioning = ARRAY['dt']
        )
        """
    )
