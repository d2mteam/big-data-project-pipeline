import json
import os
import re
import unicodedata

import mlflow
import mlflow.sklearn
import pandas as pd
import redis
from mlflow import MlflowClient
from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import KeyedProcessFunction


def main():
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
    enriched_topic = os.getenv("ENRICHED_TOPIC", "trafic.enriched_events")
    predictions_topic = os.getenv("PREDICTIONS_TOPIC", "trafic.predictions")
    group_id = os.getenv("KAFKA_GROUP_ID", "traffic-enriched-to-predictions")
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_db = int(os.getenv("REDIS_DB", "0"))
    mlflow_tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    model_name = os.getenv("KMEANS_MODEL_NAME", "traffic_state_kmeans_30m")
    model_refresh_records = int(os.getenv("MODEL_REFRESH_RECORDS", "300"))

    longterm_feature_fields = [
        "baseline_speed_7d",
        "avg_speed_hour_dow_4w",
        "avg_delay_hour_dow_4w",
        "avg_delay_same_hour_yesterday",
        "avg_speed_same_hour_yesterday",
        "source_cutoff",
    ]
    model_feature_fields = [
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

    def slug(value):
        text = str(value or "unknown").replace("Đ", "D").replace("đ", "d")
        normalized = unicodedata.normalize("NFKD", text)
        ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
        return re.sub(r"[^a-z0-9]+", "_", ascii_value.lower()).strip("_") or "unknown"

    def route_key(value):
        event = json.loads(value)
        return "|".join(
            [
                event.get("road_name", ""),
                event.get("district", ""),
                event.get("city", ""),
            ]
        )

    def redis_key(event):
        return (
            "traffic:longterm:"
            f"{slug(event.get('city'))}:"
            f"{slug(event.get('district'))}:"
            f"{slug(event.get('road_name'))}:"
            f"h{int(event.get('hour', 0)):02d}:"
            f"dow{int(event.get('day_of_week', 0))}"
        )

    def as_float(value):
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    class PredictWithRedisFeatures(KeyedProcessFunction):
        def open(self, runtime_context):
            self.redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                socket_connect_timeout=2,
                socket_timeout=2,
                decode_responses=True,
            )
            mlflow.set_tracking_uri(mlflow_tracking_uri)
            self.model = None
            self.transition_payload = None
            self.processed_records = 0
            self.reload_model()

        def reload_model(self):
            try:
                client = MlflowClient()
                version = client.get_model_version_by_alias(model_name, "champion")
                self.model = mlflow.sklearn.load_model(
                    f"models:/{model_name}@champion"
                )
                artifact_path = client.download_artifacts(
                    version.run_id, "transition_30m.json"
                )
                with open(artifact_path, "r", encoding="utf-8") as artifact:
                    self.transition_payload = json.load(artifact)
            except Exception as error:
                self.model = None
                self.transition_payload = None
                print(f"KMeans model is not available from MLflow: {error}")

        def process_element(self, value, ctx):
            event = json.loads(value)
            self.processed_records += 1
            if self.processed_records % model_refresh_records == 0:
                self.reload_model()

            feature_hash = {}
            try:
                feature_hash = self.redis_client.hgetall(redis_key(event))
            except redis.RedisError:
                feature_hash = {}

            longterm_features = {}
            for field in longterm_feature_fields:
                raw_value = feature_hash.get(field)
                if field == "source_cutoff":
                    longterm_features[field] = raw_value
                else:
                    longterm_features[field] = as_float(raw_value)

            prediction_features = dict(event)
            prediction_features.update(longterm_features)
            prediction = {
                "forecast_horizon_minutes": 30,
                "current_cluster": None,
                "predicted_cluster": None,
                "traffic_status": None,
                "expected_avg_speed": None,
                "expected_avg_delay_minutes": None,
                "model_status": "unavailable",
            }
            if self.model is not None and self.transition_payload is not None:
                row = {
                    field: as_float(prediction_features.get(field))
                    for field in model_feature_fields
                }
                current_cluster = int(
                    self.model.predict(pd.DataFrame([row], columns=model_feature_fields))[0]
                )
                transition = self.transition_payload["transition_by_current_cluster"]
                predicted_cluster = int(
                    transition.get(str(current_cluster), current_cluster)
                )
                profile = self.transition_payload["cluster_profiles"].get(
                    str(predicted_cluster), {}
                )
                prediction.update(
                    {
                        "current_cluster": current_cluster,
                        "predicted_cluster": predicted_cluster,
                        "traffic_status": profile.get("status"),
                        "expected_avg_speed": profile.get("avg_speed"),
                        "expected_avg_delay_minutes": profile.get("avg_delay_minutes"),
                        "model_status": "loaded",
                    }
                )

            output = {
                "road_name": event.get("road_name"),
                "district": event.get("district"),
                "city": event.get("city"),
                "timestamp": event.get("timestamp"),
                "prediction_30m": prediction,
            }

            yield json.dumps(output, ensure_ascii=False, separators=(",", ":"))

    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10_000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_topics(enriched_topic)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(predictions_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    enriched_events = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "enriched-events-kafka-source",
        Types.STRING(),
    )

    predictions = enriched_events.key_by(route_key, key_type=Types.STRING()).process(
        PredictWithRedisFeatures(),
        output_type=Types.STRING(),
    )

    predictions.sink_to(sink).name("predictions-kafka-sink")

    env.execute("enriched-to-kmeans-state-prediction-30m")


if __name__ == "__main__":
    main()
