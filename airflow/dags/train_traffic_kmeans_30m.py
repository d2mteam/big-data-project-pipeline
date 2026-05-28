import os
import re
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import mlflow
import mlflow.sklearn
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from mlflow import MlflowClient
from mlflow.models import infer_signature
from sklearn.cluster import KMeans
from sklearn.impute import SimpleImputer
from sklearn.metrics import silhouette_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from iceberg_utils import (
    ENRICHED_FQN,
    LONGTERM_FQN,
    TRAINING_FQN,
    ensure_iceberg_tables,
    fetch_trino_dataframe,
    fetch_trino_value,
    run_trino,
    sql_string,
)


MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_EXPERIMENT_NAME = os.getenv(
    "MLFLOW_EXPERIMENT_NAME", "traffic-state-kmeans-30m"
)
MODEL_NAME = os.getenv("KMEANS_MODEL_NAME", "traffic_state_kmeans_30m")
N_CLUSTERS = int(os.getenv("KMEANS_N_CLUSTERS", "3"))
LOCAL_TIMEZONE = ZoneInfo(os.getenv("LOCAL_TIMEZONE", "Asia/Ho_Chi_Minh"))
MIN_TRAIN_ROWS = int(os.getenv("KMEANS_MIN_TRAIN_ROWS", "50"))
WAIT_TIMEOUT_SECONDS = int(os.getenv("KMEANS_WAIT_TIMEOUT_SECONDS", "180"))
WAIT_POKE_SECONDS = int(os.getenv("KMEANS_WAIT_POKE_SECONDS", "5"))

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


def safe_name(value):
    return re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-") or "run"


def get_joinable_training_row_count():
    return fetch_trino_value(
        f"""
        SELECT count(*)
        FROM {ENRICHED_FQN} e
        JOIN {LONGTERM_FQN} f
          ON f.road_name = e.road_name
         AND f.district = e.district
         AND f.city = e.city
         AND cast(f.hour AS INTEGER) = cast(e.hour AS INTEGER)
         AND cast(f.day_of_week AS INTEGER) = cast(e.day_of_week AS INTEGER)
         AND f.source_cutoff <= e.event_ts
        WHERE e.event_ts IS NOT NULL
        """
    )


def wait_for_training_data():
    ensure_iceberg_tables()
    required_rows = max(MIN_TRAIN_ROWS, N_CLUSTERS)
    deadline = time.monotonic() + WAIT_TIMEOUT_SECONDS

    while True:
        try:
            enriched_count = fetch_trino_value(f"SELECT count(*) FROM {ENRICHED_FQN}") or 0
            longterm_count = fetch_trino_value(f"SELECT count(*) FROM {LONGTERM_FQN}") or 0
            joinable_count = get_joinable_training_row_count() or 0
        except Exception as error:
            enriched_count = 0
            longterm_count = 0
            joinable_count = 0
            print(f"Training data readiness query failed; will retry: {error}")

        print(
            "Training data readiness: "
            f"enriched={enriched_count}, "
            f"longterm={longterm_count}, "
            f"joinable={joinable_count}, "
            f"required_joinable={required_rows}"
        )

        if enriched_count > 0 and longterm_count > 0 and joinable_count >= required_rows:
            return {
                "enriched_rows": int(enriched_count),
                "longterm_rows": int(longterm_count),
                "joinable_rows": int(joinable_count),
                "required_rows": int(required_rows),
            }

        if time.monotonic() >= deadline:
            raise AirflowException(
                "Timed out waiting for enough KMeans training data: "
                f"enriched={enriched_count}, "
                f"longterm={longterm_count}, "
                f"joinable={joinable_count}, "
                f"required_joinable={required_rows}, "
                f"timeout_seconds={WAIT_TIMEOUT_SECONDS}"
            )

        time.sleep(WAIT_POKE_SECONDS)


def build_training_dataset(**context):
    ensure_iceberg_tables()
    enriched_count = fetch_trino_value(f"SELECT count(*) FROM {ENRICHED_FQN}")
    if not enriched_count:
        raise AirflowException("No Iceberg enriched events are available.")

    longterm_count = fetch_trino_value(f"SELECT count(*) FROM {LONGTERM_FQN}")
    if not longterm_count:
        raise AirflowException("No Iceberg long-term features are available.")

    training_run_id = safe_name(context["run_id"])
    run_trino(
        f"DELETE FROM {TRAINING_FQN} WHERE training_run_id = {sql_string(training_run_id)}"
    )
    run_trino(
        f"""
        INSERT INTO {TRAINING_FQN}
        WITH enriched AS (
            SELECT DISTINCT
                event_ts,
                road_name,
                district,
                city,
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
            FROM {ENRICHED_FQN}
            WHERE event_ts IS NOT NULL
        ),
        longterm AS (
            SELECT DISTINCT
                road_name,
                district,
                city,
                cast(hour AS INTEGER) AS hour,
                cast(day_of_week AS INTEGER) AS day_of_week,
                cast(baseline_speed_7d AS DOUBLE) AS baseline_speed_7d,
                cast(avg_speed_hour_dow_4w AS DOUBLE) AS avg_speed_hour_dow_4w,
                cast(avg_delay_hour_dow_4w AS DOUBLE) AS avg_delay_hour_dow_4w,
                cast(avg_delay_same_hour_yesterday AS DOUBLE)
                    AS avg_delay_same_hour_yesterday,
                cast(avg_speed_same_hour_yesterday AS DOUBLE)
                    AS avg_speed_same_hour_yesterday,
                source_cutoff
            FROM {LONGTERM_FQN}
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
        SELECT
            {sql_string(training_run_id)} AS training_run_id,
            cast(event_ts AS DATE) AS dt,
            cast(current_timestamp AS TIMESTAMP(3)) AS created_at,
            event_ts,
            road_name,
            district,
            city,
            vehicle_count,
            avg_speed,
            min_speed,
            max_speed,
            avg_delay_minutes,
            truck_ratio,
            bus_ratio,
            motorbike_ratio,
            car_ratio,
            taxi_ratio,
            accident_count,
            temperature_celsius,
            humidity_percentage,
            is_rain,
            hour,
            day_of_week,
            is_weekend,
            is_peak_hour,
            prev_avg_speed_1,
            prev_avg_speed_2,
            prev_avg_speed_3,
            prev_vehicle_count_1,
            prev_vehicle_count_2,
            prev_vehicle_count_3,
            prev_delay_1,
            prev_delay_2,
            prev_delay_3,
            rolling_avg_speed_3,
            rolling_vehicle_count_3,
            rolling_delay_3,
            baseline_speed_7d,
            avg_speed_hour_dow_4w,
            avg_delay_hour_dow_4w,
            avg_delay_same_hour_yesterday,
            avg_speed_same_hour_yesterday,
            feature_source_cutoff
        FROM joined
        WHERE feature_rank = 1
        """
    )
    row_count = fetch_trino_value(
        f"""
        SELECT count(*)
        FROM {TRAINING_FQN}
        WHERE training_run_id = {sql_string(training_run_id)}
        """
    )
    if row_count < N_CLUSTERS:
        raise AirflowException("Not enough records to train KMeans.")

    print(f"Wrote {row_count} training rows to {TRAINING_FQN}.")
    return training_run_id


def train_kmeans_30m(**context):
    training_run_id = context["ti"].xcom_pull(task_ids="build_training_dataset")
    if not training_run_id:
        raise AirflowException("Training dataset was not generated.")

    dataset = fetch_trino_dataframe(
        f"""
        SELECT *
        FROM {TRAINING_FQN}
        WHERE training_run_id = {sql_string(training_run_id)}
        ORDER BY event_ts
        """
    )
    if len(dataset) < N_CLUSTERS:
        raise AirflowException("Not enough records to train KMeans.")

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
    future["event_ts"] = future["event_ts"] - timedelta(minutes=30)
    transitions = dataset[["road_name", "district", "city", "event_ts", "cluster"]].merge(
        future,
        on=["road_name", "district", "city", "event_ts"],
        how="inner",
    )
    if transitions.empty:
        raise AirflowException("No 30-minute transitions are available for training.")

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
                "training_run_id": training_run_id,
                "training_table": TRAINING_FQN,
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
    tags=["traffic", "training", "kmeans", "mlflow", "30m", "iceberg", "trino"],
) as dag:
    wait_for_data_task = PythonOperator(
        task_id="wait_for_training_data",
        python_callable=wait_for_training_data,
    )
    build_dataset_task = PythonOperator(
        task_id="build_training_dataset",
        python_callable=build_training_dataset,
    )
    train_task = PythonOperator(
        task_id="train_kmeans_30m",
        python_callable=train_kmeans_30m,
    )
    wait_for_data_task >> build_dataset_task >> train_task
