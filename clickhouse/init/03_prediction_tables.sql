CREATE DATABASE IF NOT EXISTS traffic_analytics;
CREATE TABLE IF NOT EXISTS traffic_analytics.traffic_predictions (
    prediction_time DateTime,
    road_name String DEFAULT '',
    district String DEFAULT '',
    city String DEFAULT '',
    forecast_horizon_minutes Int32,
    current_cluster Nullable(Int32),
    predicted_cluster Nullable(Int32),
    predicted_traffic_status String DEFAULT '',
    expected_avg_speed Nullable(Float64),
    expected_avg_delay_minutes Nullable(Float64),
    model_status String DEFAULT '',
    source_topic String,
    raw_message String
) ENGINE = MergeTree
ORDER BY (prediction_time, city, district, road_name);
CREATE TABLE IF NOT EXISTS traffic_analytics.predictions_queue (raw_message String) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka:29092',
kafka_topic_list = 'trafic.predictions',
kafka_group_name = 'clickhouse-traffic-predictions',
kafka_format = 'JSONAsString',
kafka_num_consumers = 1;
CREATE MATERIALIZED VIEW IF NOT EXISTS traffic_analytics.predictions_mv TO traffic_analytics.traffic_predictions AS
SELECT coalesce(
        parseDateTimeBestEffortOrNull(
            nullIf(JSONExtractString(raw_message, 'timestamp'), '')
        ),
        now()
    ) AS prediction_time,
    ifNull(
        nullIf(JSONExtractString(raw_message, 'road_name'), ''),
        ''
    ) AS road_name,
    ifNull(
        nullIf(JSONExtractString(raw_message, 'district'), ''),
        ''
    ) AS district,
    ifNull(
        nullIf(JSONExtractString(raw_message, 'city'), ''),
        ''
    ) AS city,
    toInt32OrZero(
        JSONExtractRaw(
            JSONExtractRaw(raw_message, 'prediction_30m'),
            'forecast_horizon_minutes'
        )
    ) AS forecast_horizon_minutes,
    toInt32OrNull(
        JSONExtractRaw(
            JSONExtractRaw(raw_message, 'prediction_30m'),
            'current_cluster'
        )
    ) AS current_cluster,
    toInt32OrNull(
        JSONExtractRaw(
            JSONExtractRaw(raw_message, 'prediction_30m'),
            'predicted_cluster'
        )
    ) AS predicted_cluster,
    ifNull(
        nullIf(
            JSONExtractString(
                JSONExtractRaw(raw_message, 'prediction_30m'),
                'traffic_status'
            ),
            ''
        ),
        ''
    ) AS predicted_traffic_status,
    toFloat64OrNull(
        JSONExtractRaw(
            JSONExtractRaw(raw_message, 'prediction_30m'),
            'expected_avg_speed'
        )
    ) AS expected_avg_speed,
    toFloat64OrNull(
        JSONExtractRaw(
            JSONExtractRaw(raw_message, 'prediction_30m'),
            'expected_avg_delay_minutes'
        )
    ) AS expected_avg_delay_minutes,
    ifNull(
        nullIf(
            JSONExtractString(
                JSONExtractRaw(raw_message, 'prediction_30m'),
                'model_status'
            ),
            ''
        ),
        ''
    ) AS model_status,
    'trafic.predictions' AS source_topic,
    raw_message
FROM traffic_analytics.predictions_queue;