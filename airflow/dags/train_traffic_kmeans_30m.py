import json
import os
import re
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import boto3
import duckdb
import mlflow
import mlflow.sklearn
import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from mlflow import MlflowClient
from mlflow.models import infer_signature
from sklearn.cluster import KMeans
from sklearn.impute import SimpleImputer
from sklearn.metrics import silhouette_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "traffic-lake")
MINIO_PREFIX = os.getenv("MINIO_PREFIX", "traffic/enriched_events")
LONGTERM_FEATURE_PREFIX = os.getenv(
    "LONGTERM_FEATURE_PREFIX", "traffic/longterm_features/staging"
)
TRAINING_DATASET_PREFIX = os.getenv(
    "TRAINING_DATASET_PREFIX", "traffic/training_datasets/kmeans_30m"
)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_EXPERIMENT_NAME = os.getenv(
    "MLFLOW_EXPERIMENT_NAME", "traffic-state-kmeans-30m"
)
MODEL_NAME = os.getenv("KMEANS_MODEL_NAME", "traffic_state_kmeans_30m")
N_CLUSTERS = int(os.getenv("KMEANS_N_CLUSTERS", "3"))
LOCAL_TIMEZONE = ZoneInfo(os.getenv("LOCAL_TIMEZONE", "Asia/Ho_Chi_Minh"))

FEATURE_COLUMNS = [
    "vehicle_count",
    "avg_speed",
    "min_speed",
    "max_speed",
    "avg_delay_minutes",
    "truck_ratio",
    "bus_ratio",
    "motorbike_ratio",
    "car_ratio",
    "taxi_ratio",
    "accident_count",
    "temperature_celsius",
    "humidity_percentage",
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
    "baseline_speed_7d",
    "avg_speed_hour_dow_4w",
    "avg_delay_hour_dow_4w",
    "avg_delay_same_hour_yesterday",
    "avg_speed_same_hour_yesterday",
]


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


def duckdb_list(paths):
    return "[" + ",".join("'" + path.replace("'", "''") + "'" for path in paths) + "]"


def download_files(client, keys, directory, prefix):
    paths = []
    for index, key in enumerate(keys):
        local_file = Path(directory) / f"{prefix}_{index:06d}.parquet"
        client.download_file(MINIO_BUCKET, key, str(local_file))
        paths.append(str(local_file))
    return paths


def build_training_dataset(**context):
    client = s3_client()
    enriched_keys = list_parquet_keys(client, MINIO_PREFIX)
    feature_keys = list_parquet_keys(client, LONGTERM_FEATURE_PREFIX)
    if not enriched_keys:
        raise AirflowSkipException("No enriched event Parquet files are available.")
    if not feature_keys:
        raise AirflowSkipException("No long-term feature Parquet files are available.")

    logical_time = context["logical_date"].astimezone(LOCAL_TIMEZONE)
    output_key = (
        f"{TRAINING_DATASET_PREFIX}/dt={logical_time:%Y-%m-%d}/"
        f"training_dataset_{safe_name(context['run_id'])}.parquet"
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        enriched_paths = download_files(client, enriched_keys, temp_dir, "enriched")
        feature_paths = download_files(client, feature_keys, temp_dir, "longterm")
        output_file = Path(temp_dir) / "kmeans_training_dataset.parquet"
        query = f"""
        COPY (
            WITH enriched AS (
                SELECT DISTINCT
                    try_cast("timestamp" AS TIMESTAMP) AS event_ts,
                    road_name, district, city,
                    cast(vehicle_count AS DOUBLE) AS vehicle_count,
                    cast(avg_speed AS DOUBLE) AS avg_speed,
                    cast(min_speed AS DOUBLE) AS min_speed,
                    cast(max_speed AS DOUBLE) AS max_speed,
                    cast(avg_delay_minutes AS DOUBLE) AS avg_delay_minutes,
                    cast(truck_ratio AS DOUBLE) AS truck_ratio,
                    cast(bus_ratio AS DOUBLE) AS bus_ratio,
                    cast(motorbike_ratio AS DOUBLE) AS motorbike_ratio,
                    cast(car_ratio AS DOUBLE) AS car_ratio,
                    cast(taxi_ratio AS DOUBLE) AS taxi_ratio,
                    cast(accident_count AS DOUBLE) AS accident_count,
                    cast(temperature_celsius AS DOUBLE) AS temperature_celsius,
                    cast(humidity_percentage AS DOUBLE) AS humidity_percentage,
                    cast(is_rain AS DOUBLE) AS is_rain,
                    cast(hour AS INTEGER) AS hour,
                    cast(day_of_week AS INTEGER) AS day_of_week,
                    cast(is_weekend AS DOUBLE) AS is_weekend,
                    cast(is_peak_hour AS DOUBLE) AS is_peak_hour,
                    cast(prev_avg_speed_1 AS DOUBLE) AS prev_avg_speed_1,
                    cast(prev_avg_speed_2 AS DOUBLE) AS prev_avg_speed_2,
                    cast(prev_avg_speed_3 AS DOUBLE) AS prev_avg_speed_3,
                    cast(prev_vehicle_count_1 AS DOUBLE) AS prev_vehicle_count_1,
                    cast(prev_vehicle_count_2 AS DOUBLE) AS prev_vehicle_count_2,
                    cast(prev_vehicle_count_3 AS DOUBLE) AS prev_vehicle_count_3,
                    cast(prev_delay_1 AS DOUBLE) AS prev_delay_1,
                    cast(prev_delay_2 AS DOUBLE) AS prev_delay_2,
                    cast(prev_delay_3 AS DOUBLE) AS prev_delay_3,
                    cast(rolling_avg_speed_3 AS DOUBLE) AS rolling_avg_speed_3,
                    cast(rolling_vehicle_count_3 AS DOUBLE) AS rolling_vehicle_count_3,
                    cast(rolling_delay_3 AS DOUBLE) AS rolling_delay_3
                FROM read_parquet({duckdb_list(enriched_paths)}, union_by_name=true)
                WHERE "timestamp" IS NOT NULL
            ),
            longterm AS (
                SELECT DISTINCT
                    road_name, district, city,
                    cast(hour AS INTEGER) AS hour,
                    cast(day_of_week AS INTEGER) AS day_of_week,
                    cast(baseline_speed_7d AS DOUBLE) AS baseline_speed_7d,
                    cast(avg_speed_hour_dow_4w AS DOUBLE) AS avg_speed_hour_dow_4w,
                    cast(avg_delay_hour_dow_4w AS DOUBLE) AS avg_delay_hour_dow_4w,
                    cast(avg_delay_same_hour_yesterday AS DOUBLE)
                        AS avg_delay_same_hour_yesterday,
                    cast(avg_speed_same_hour_yesterday AS DOUBLE)
                        AS avg_speed_same_hour_yesterday,
                    try_cast(source_cutoff AS TIMESTAMP) AS source_cutoff
                FROM read_parquet({duckdb_list(feature_paths)}, union_by_name=true)
            ),
            joined AS (
                SELECT
                    e.*,
                    ft.baseline_speed_7d,
                    ft.avg_speed_hour_dow_4w,
                    ft.avg_delay_hour_dow_4w,
                    ft.avg_delay_same_hour_yesterday,
                    ft.avg_speed_same_hour_yesterday,
                    ft.source_cutoff AS feature_source_cutoff,
                    row_number() OVER (
                        PARTITION BY e.road_name, e.district, e.city, e.event_ts
                        ORDER BY ft.source_cutoff DESC NULLS LAST
                    ) AS feature_rank
                FROM enriched e
                LEFT JOIN longterm ft
                  ON ft.road_name = e.road_name
                 AND ft.district = e.district
                 AND ft.city = e.city
                 AND ft.hour = e.hour
                 AND ft.day_of_week = e.day_of_week
                 AND ft.source_cutoff <= e.event_ts
            )
            SELECT * EXCLUDE (feature_rank)
            FROM joined
            WHERE feature_rank = 1
            ORDER BY event_ts, city, district, road_name
        ) TO '{str(output_file).replace("'", "''")}' (FORMAT PARQUET)
        """
        duckdb.connect(database=":memory:").execute(query)
        dataset = pd.read_parquet(output_file)
        if len(dataset) < N_CLUSTERS:
            raise AirflowSkipException("Not enough records to train KMeans.")
        client.upload_file(str(output_file), MINIO_BUCKET, output_key)

    print(f"Wrote {len(dataset)} joined feature rows to s3://{MINIO_BUCKET}/{output_key}")
    return output_key


def train_kmeans_30m(**context):
    dataset_key = context["ti"].xcom_pull(task_ids="build_training_dataset")
    if not dataset_key:
        raise AirflowSkipException("Training dataset was not generated.")

    client = s3_client()
    with tempfile.NamedTemporaryFile(suffix=".parquet") as temp_file:
        client.download_file(MINIO_BUCKET, dataset_key, temp_file.name)
        dataset = pd.read_parquet(temp_file.name).sort_values("event_ts")

    features = dataset[FEATURE_COLUMNS]
    model = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            (
                "kmeans",
                KMeans(n_clusters=N_CLUSTERS, random_state=42, n_init=10),
            ),
        ]
    )
    dataset["cluster"] = model.fit_predict(features)

    profiles = (
        dataset.groupby("cluster", as_index=False)
        .agg(
            avg_speed=("avg_speed", "mean"),
            avg_delay_minutes=("avg_delay_minutes", "mean"),
            vehicle_count=("vehicle_count", "mean"),
            samples=("cluster", "size"),
        )
        .sort_values("avg_delay_minutes")
    )
    status_names = ["clear", "moderate", "congested"]
    profile_payload = {}
    for rank, row in enumerate(profiles.to_dict(orient="records")):
        cluster = str(int(row["cluster"]))
        status = status_names[min(rank, len(status_names) - 1)]
        profile_payload[cluster] = {
            "status": status,
            "avg_speed": float(row["avg_speed"]),
            "avg_delay_minutes": float(row["avg_delay_minutes"]),
            "vehicle_count": float(row["vehicle_count"]),
            "samples": int(row["samples"]),
        }

    future = dataset[["road_name", "district", "city", "event_ts", "cluster"]].rename(
        columns={"cluster": "future_cluster"}
    )
    future["event_ts"] = future["event_ts"] - pd.Timedelta(minutes=30)
    # Pair each current cluster with the cluster observed on the same route 30 minutes later.
    transitions = dataset[["road_name", "district", "city", "event_ts", "cluster"]].merge(
        future,
        on=["road_name", "district", "city", "event_ts"],
        how="inner",
    )
    if transitions.empty:
        raise AirflowSkipException("No 30-minute transitions are available for training.")

    counts = (
        transitions.groupby(["cluster", "future_cluster"])
        .size()
        .reset_index(name="count")
        .sort_values(["cluster", "count"], ascending=[True, False])
    )
    mapping = {
        str(int(row["cluster"])): int(row["future_cluster"])
        for row in counts.drop_duplicates("cluster").to_dict(orient="records")
    }
    for cluster in range(N_CLUSTERS):
        mapping.setdefault(str(cluster), cluster)

    transition_payload = {
        "horizon_minutes": 30,
        "transition_by_current_cluster": mapping,
        "cluster_profiles": profile_payload,
    }
    metrics = {
        "inertia": float(model.named_steps["kmeans"].inertia_),
        "transition_rows": float(len(transitions)),
    }
    if len(dataset) > N_CLUSTERS and dataset["cluster"].nunique() > 1:
        transformed = model.named_steps["scaler"].transform(
            model.named_steps["imputer"].transform(features)
        )
        metrics["silhouette_score"] = float(
            silhouette_score(transformed, dataset["cluster"])
        )

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)
    with mlflow.start_run(run_name="kmeans_state_transition_30m") as run:
        mlflow.set_tags(
            {"forecast_horizon": "30m", "training_mode": "unsupervised_transition"}
        )
        mlflow.log_params(
            {
                "n_clusters": N_CLUSTERS,
                "feature_count": len(FEATURE_COLUMNS),
                "training_rows": len(dataset),
                "dataset_key": dataset_key,
            }
        )
        mlflow.log_metrics(metrics)
        mlflow.log_dict(transition_payload, "transition_30m.json")
        signature = infer_signature(features, model.predict(features))
        model_info = mlflow.sklearn.log_model(
            model,
            artifact_path="model",
            signature=signature,
            input_example=features.head(5),
            registered_model_name=MODEL_NAME,
        )

    client = MlflowClient()
    version = client.search_model_versions(f"run_id = '{run.info.run_id}'")[0].version
    client.set_registered_model_alias(MODEL_NAME, "champion", version)
    print(f"Registered {model_info.model_uri}; set {MODEL_NAME}@champion to version {version}.")
    return run.info.run_id


with DAG(
    dag_id="train_traffic_kmeans_30m",
    description="Train KMeans traffic-state clusters and 30-minute transition forecast.",
    start_date=datetime(2026, 1, 1, tzinfo=LOCAL_TIMEZONE),
    schedule="0 2 * * 1",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    tags=["traffic", "training", "kmeans", "mlflow", "30m"],
) as dag:
    build_dataset_task = PythonOperator(
        task_id="build_training_dataset",
        python_callable=build_training_dataset,
    )
    train_task = PythonOperator(
        task_id="train_kmeans_30m",
        python_callable=train_kmeans_30m,
    )
    build_dataset_task >> train_task
