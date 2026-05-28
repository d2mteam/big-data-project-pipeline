import json
import os

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
from pyflink.datastream.state import ListStateDescriptor


def main():
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
    enriched_topic = os.getenv("ENRICHED_TOPIC", "trafic.enriched_events")
    alert_topic = os.getenv("ACCIDENT_ALERT_TOPIC", "trafic.accident_anomaly_alerts")
    group_id = os.getenv("KAFKA_GROUP_ID", "traffic-accident-anomaly-alerts")
    history_size = int(os.getenv("ACCIDENT_HISTORY_EVENTS", "6"))
    spike_multiplier = float(os.getenv("ACCIDENT_SPIKE_MULTIPLIER", "3"))

    def route_key(value):
        event = json.loads(value)
        return "|".join(
            [
                event.get("road_name", ""),
                event.get("district", ""),
                event.get("city", ""),
            ]
        )

    class DetectAccidentSpike(KeyedProcessFunction):
        def open(self, runtime_context):
            self.history = runtime_context.get_list_state(
                ListStateDescriptor("accident_count_history", Types.INT())
            )

        def process_element(self, value, ctx):
            event = json.loads(value)
            current = int(float(event.get("accident_count") or 0))
            history = list(self.history.get())
            baseline = (sum(history) / len(history)) if history else 0.0
            previous_max = max(history) if history else 0

            is_first_accident_after_clean = current > 0 and previous_max == 0
            is_spike = current > 0 and current >= max(1, baseline * spike_multiplier)
            is_multi_accident = current >= 2

            if is_first_accident_after_clean or is_spike or is_multi_accident:
                alert = {
                    "alert_type": "accident_anomaly",
                    "severity": "high" if current >= 2 else "medium",
                    "road_name": event.get("road_name"),
                    "district": event.get("district"),
                    "city": event.get("city"),
                    "timestamp": event.get("timestamp"),
                    "accident_count": current,
                    "history_avg_accident_count": round(baseline, 3),
                    "history_max_accident_count": previous_max,
                    "avg_speed": event.get("avg_speed"),
                    "vehicle_count": event.get("vehicle_count"),
                    "rule": f"current_gt_0_and_spike_multiplier_{spike_multiplier}",
                    "matched_by": {
                        "first_accident_after_clean_history": is_first_accident_after_clean,
                        "spike_vs_history": is_spike,
                        "multi_accident": is_multi_accident,
                    },
                }
                yield json.dumps(alert, ensure_ascii=False, separators=(",", ":"))

            next_history = (history + [current])[-history_size:]
            self.history.update(next_history)

    env = StreamExecutionEnvironment.get_execution_environment()

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
            .set_topic(alert_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "enriched-events-kafka-source",
        Types.STRING(),
    )

    alerts = stream.key_by(route_key, key_type=Types.STRING()).process(
        DetectAccidentSpike(),
        output_type=Types.STRING(),
    )
    alerts.sink_to(sink).name("accident-anomaly-alerts-kafka-sink")

    env.execute("accident-anomaly-alerts")


if __name__ == "__main__":
    main()
