import json
import os
from datetime import datetime

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
from pyflink.datastream.state import ValueStateDescriptor


def main():
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
    enriched_topic = os.getenv("ENRICHED_TOPIC", "trafic.enriched_events")
    alert_topic = os.getenv("CONGESTION_ALERT_TOPIC", "trafic.congestion_alerts")
    group_id = os.getenv("KAFKA_GROUP_ID", "traffic-congestion-cep-alerts")
    speed_threshold = float(os.getenv("CONGESTION_SPEED_THRESHOLD", "10"))
    consecutive_threshold = int(os.getenv("CONGESTION_CONSECUTIVE_EVENTS", "3"))
    duration_threshold_minutes = int(os.getenv("CONGESTION_DURATION_MINUTES", "30"))

    def route_key(value):
        event = json.loads(value)
        return "|".join(
            [
                event.get("road_name", ""),
                event.get("district", ""),
                event.get("city", ""),
            ]
        )

    def parse_ts(value):
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            return datetime.utcnow()

    class DetectLongCongestion(KeyedProcessFunction):
        def open(self, runtime_context):
            self.low_count = runtime_context.get_state(
                ValueStateDescriptor("low_speed_count", Types.INT())
            )
            self.first_low_ts = runtime_context.get_state(
                ValueStateDescriptor("first_low_ts", Types.STRING())
            )

        def process_element(self, value, ctx):
            event = json.loads(value)
            avg_speed = float(event.get("avg_speed") or 0)
            event_ts = parse_ts(event.get("timestamp"))

            if avg_speed >= speed_threshold:
                self.low_count.clear()
                self.first_low_ts.clear()
                return

            count = (self.low_count.value() or 0) + 1
            first_ts_raw = self.first_low_ts.value()
            if not first_ts_raw:
                first_ts_raw = event_ts.isoformat()
                self.first_low_ts.update(first_ts_raw)
            self.low_count.update(count)

            first_ts = parse_ts(first_ts_raw)
            duration_minutes = int((event_ts - first_ts).total_seconds() / 60)
            matched_by_count = count >= consecutive_threshold
            matched_by_duration = duration_minutes >= duration_threshold_minutes

            if matched_by_count or matched_by_duration:
                alert = {
                    "alert_type": "long_congestion",
                    "severity": "high" if avg_speed < 7 else "medium",
                    "road_name": event.get("road_name"),
                    "district": event.get("district"),
                    "city": event.get("city"),
                    "timestamp": event.get("timestamp"),
                    "avg_speed": avg_speed,
                    "vehicle_count": event.get("vehicle_count"),
                    "low_speed_event_count": count,
                    "low_speed_duration_minutes": max(duration_minutes, 0),
                    "rule": f"{consecutive_threshold}_events_or_{duration_threshold_minutes}m_avg_speed_lt_{speed_threshold}",
                    "matched_by": {
                        "consecutive_events": matched_by_count,
                        "duration": matched_by_duration,
                    },
                }
                yield json.dumps(alert, ensure_ascii=False, separators=(",", ":"))

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
        DetectLongCongestion(),
        output_type=Types.STRING(),
    )
    alerts.sink_to(sink).name("congestion-alerts-kafka-sink")

    env.execute("congestion-cep-alerts")


if __name__ == "__main__":
    main()
