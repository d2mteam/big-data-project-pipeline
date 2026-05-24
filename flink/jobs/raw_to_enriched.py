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
    raw_topic = os.getenv("RAW_TOPIC", "trafic.raw_events")
    enriched_topic = os.getenv("ENRICHED_TOPIC", "trafic.enriched_events")
    group_id = os.getenv("KAFKA_GROUP_ID", "traffic-raw-to-enriched")

    def route_key(value):
        event = json.loads(value)
        return "|".join(
            [
                event.get("road_name", ""),
                event.get("district", ""),
                event.get("city", ""),
            ]
        )

    def num(record, field):
        if record is None:
            return None
        value = record.get(field)
        return float(value) if value is not None else None

    def avg(records, field):
        values = [num(record, field) for record in records]
        values = [value for value in values if value is not None]
        if not values:
            return None
        return sum(values) / len(values)

    class EnrichWithHistory(KeyedProcessFunction):
        def open(self, runtime_context):
            self.history = runtime_context.get_list_state(
                ListStateDescriptor("last_3_events", Types.STRING())
            )

        def process_element(self, value, ctx):
            event = json.loads(value)
            history = [json.loads(item) for item in self.history.get()]

            prev_1 = history[-1] if len(history) >= 1 else None
            prev_2 = history[-2] if len(history) >= 2 else None
            prev_3 = history[-3] if len(history) >= 3 else None
            rolling_records = history[-2:] + [event]

            enriched = dict(event)
            enriched["prev_avg_speed_1"] = num(prev_1, "avg_speed")
            enriched["prev_avg_speed_2"] = num(prev_2, "avg_speed")
            enriched["prev_avg_speed_3"] = num(prev_3, "avg_speed")
            enriched["prev_vehicle_count_1"] = num(prev_1, "vehicle_count")
            enriched["prev_vehicle_count_2"] = num(prev_2, "vehicle_count")
            enriched["prev_vehicle_count_3"] = num(prev_3, "vehicle_count")
            enriched["prev_delay_1"] = num(prev_1, "avg_delay_minutes")
            enriched["prev_delay_2"] = num(prev_2, "avg_delay_minutes")
            enriched["prev_delay_3"] = num(prev_3, "avg_delay_minutes")
            enriched["rolling_avg_speed_3"] = avg(rolling_records, "avg_speed")
            enriched["rolling_vehicle_count_3"] = avg(
                rolling_records, "vehicle_count"
            )
            enriched["rolling_delay_3"] = avg(rolling_records, "avg_delay_minutes")

            next_history = (history + [event])[-3:]
            self.history.update(
                [
                    json.dumps(item, ensure_ascii=False, separators=(",", ":"))
                    for item in next_history
                ]
            )

            yield json.dumps(enriched, ensure_ascii=False, separators=(",", ":"))

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(10_000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_topics(raw_topic)
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
            .set_topic(enriched_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    raw_events = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "raw-events-kafka-source",
        Types.STRING(),
    )

    enriched_events = raw_events.key_by(route_key, key_type=Types.STRING()).process(
        EnrichWithHistory(),
        output_type=Types.STRING(),
    )

    enriched_events.sink_to(sink).name("enriched-events-kafka-sink")

    env.execute("raw-to-enriched-state-window")


if __name__ == "__main__":
    main()
