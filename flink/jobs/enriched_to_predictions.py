import json
import os
import random
import re
import unicodedata

import redis
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

    feature_fields = [
        "baseline_speed_7d",
        "avg_speed_hour_dow_4w",
        "avg_delay_hour_dow_4w",
        "avg_delay_same_hour_yesterday",
        "avg_speed_same_hour_yesterday",
        "source_cutoff",
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

    def random_speed():
        return round(random.uniform(5.0, 55.0), 2)

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

        def process_element(self, value, ctx):
            event = json.loads(value)

            feature_hash = {}
            try:
                feature_hash = self.redis_client.hgetall(redis_key(event))
            except redis.RedisError:
                feature_hash = {}

            longterm_features = {}
            for field in feature_fields:
                raw_value = feature_hash.get(field)
                if field == "source_cutoff":
                    longterm_features[field] = raw_value
                else:
                    longterm_features[field] = as_float(raw_value)

            # Reserved for the future XGBoost call; do not emit this to Kafka.
            prediction_features = dict(event)
            prediction_features.update(longterm_features)

            output = {
                "road_name": event.get("road_name"),
                "district": event.get("district"),
                "city": event.get("city"),
                "timestamp": event.get("timestamp"),
                "predictions": {
                    "target_15m": random_speed(),
                    "target_30m": random_speed(),
                    "target_60m": random_speed(),
                },
            }

            yield json.dumps(output, ensure_ascii=False, separators=(",", ":"))

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
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

    env.execute("enriched-to-predictions-redis-xgboost-placeholder")


if __name__ == "__main__":
    main()
