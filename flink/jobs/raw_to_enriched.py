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
from pyflink.datastream.functions import (
    CoProcessFunction,
    KeyedProcessFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor


def main():
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
    sensor_topic = os.getenv("SENSOR_TOPIC", "trafic.raw_sensor")
    weather_topic = os.getenv("WEATHER_TOPIC", "trafic.raw_weather")
    enriched_topic = os.getenv("ENRICHED_TOPIC", "trafic.enriched_events")
    group_id = os.getenv("KAFKA_GROUP_ID", "traffic-raw-to-enriched")

    WEATHER_FIELDS = {
        "accident_count", "temperature_celsius", "humidity_percentage",
        "weather_condition", "is_rain",
    }

    def route_key(value):
        event = json.loads(value)
        return "|".join([
            event.get("road_name", ""),
            event.get("district", ""),
            event.get("city", ""),
        ])

    def district_city_key(value):
        event = json.loads(value)
        return "|".join([event.get("district", ""), event.get("city", "")])

    def num(record, field):
        if record is None:
            return None
        value = record.get(field)
        return float(value) if value is not None else None

    def avg(records, field):
        values = [num(r, field) for r in records]
        values = [v for v in values if v is not None]
        return sum(values) / len(values) if values else None

    class JoinWeatherToSensor(CoProcessFunction):
        def open(self, runtime_context):
            self.weather_state = runtime_context.get_state(
                ValueStateDescriptor("latest_weather", Types.STRING())
            )

        def process_element1(self, sensor_value, ctx):
            sensor = json.loads(sensor_value)
            weather_raw = self.weather_state.value()
            if weather_raw:
                weather = json.loads(weather_raw)
                for field in WEATHER_FIELDS:
                    sensor.setdefault(field, weather.get(field))
            yield json.dumps(sensor, ensure_ascii=False, separators=(",", ":"))

        def process_element2(self, weather_value, ctx):
            self.weather_state.update(weather_value)

    class AddLagFeaturesWithState(KeyedProcessFunction):
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

            next_history = (history + [event])[-3:]
            self.history.update([
                json.dumps(item, ensure_ascii=False, separators=(",", ":"))
                for item in next_history
            ])

            yield json.dumps(enriched, ensure_ascii=False, separators=(",", ":"))

    class AddRollingFeaturesWithWindow(ProcessWindowFunction):
        def process(self, key, context, elements):
            records = [json.loads(item) for item in elements]
            records.sort(key=lambda r: r.get("timestamp", ""))

            enriched = dict(records[-1])
            enriched["rolling_avg_speed_3"] = avg(records, "avg_speed")
            enriched["rolling_vehicle_count_3"] = avg(records, "vehicle_count")
            enriched["rolling_delay_3"] = avg(records, "avg_delay_minutes")

            yield json.dumps(enriched, ensure_ascii=False, separators=(",", ":"))

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    sensor_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_topics(sensor_topic)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    weather_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_topics(weather_topic)
        .set_group_id(group_id + "-weather")
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

    sensor_stream = env.from_source(
        sensor_source,
        WatermarkStrategy.no_watermarks(),
        "sensor-events-kafka-source",
        Types.STRING(),
    )

    weather_stream = env.from_source(
        weather_source,
        WatermarkStrategy.no_watermarks(),
        "weather-events-kafka-source",
        Types.STRING(),
    )

    merged_stream = (
        sensor_stream.connect(weather_stream)
        .key_by(district_city_key, district_city_key, key_type=Types.STRING())
        .process(JoinWeatherToSensor(), output_type=Types.STRING())
    )

    events_with_lag = merged_stream.key_by(route_key, key_type=Types.STRING()).process(
        AddLagFeaturesWithState(),
        output_type=Types.STRING(),
    )

    enriched_events = (
        events_with_lag.key_by(route_key, key_type=Types.STRING())
        .count_window(3, 1)
        .process(AddRollingFeaturesWithWindow(), output_type=Types.STRING())
    )

    enriched_events.sink_to(sink).name("enriched-events-kafka-sink")

    env.execute("raw-to-enriched-state-window")


if __name__ == "__main__":
    main()
