CREATE DATABASE IF NOT EXISTS traffic_analytics;
CREATE TABLE IF NOT EXISTS traffic_analytics.traffic_alerts (
    alert_time DateTime,
    alert_category String,
    road_name String DEFAULT '',
    district String DEFAULT '',
    city String DEFAULT '',
    severity String DEFAULT '',
    avg_speed Nullable(Float64),
    avg_delay_minutes Nullable(Float64),
    accident_count Nullable(Int64),
    message String DEFAULT '',
    source_topic String,
    raw_message String
) ENGINE = MergeTree
ORDER BY (
        alert_time,
        alert_category,
        city,
        district,
        road_name
    );
CREATE TABLE IF NOT EXISTS traffic_analytics.congestion_alerts_queue (raw_message String) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka:29092',
kafka_topic_list = 'trafic.congestion_alerts',
kafka_group_name = 'clickhouse-congestion-alerts',
kafka_format = 'JSONAsString',
kafka_num_consumers = 1;
CREATE TABLE IF NOT EXISTS traffic_analytics.accident_alerts_queue (raw_message String) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka:29092',
kafka_topic_list = 'trafic.accident_anomaly_alerts',
kafka_group_name = 'clickhouse-accident-alerts',
kafka_format = 'JSONAsString',
kafka_num_consumers = 1;
CREATE MATERIALIZED VIEW IF NOT EXISTS traffic_analytics.congestion_alerts_mv TO traffic_analytics.traffic_alerts AS
SELECT coalesce(
        parseDateTimeBestEffortOrNull(
            nullIf(JSONExtractString(raw_message, 'alert_time'), '')
        ),
        parseDateTimeBestEffortOrNull(
            nullIf(JSONExtractString(raw_message, 'timestamp'), '')
        ),
        parseDateTimeBestEffortOrNull(
            nullIf(JSONExtractString(raw_message, 'event_time'), '')
        ),
        now()
    ) AS alert_time,
    'congestion' AS alert_category,
    coalesce(
        nullIf(JSONExtractString(raw_message, 'road_name'), ''),
        nullIf(JSONExtractString(raw_message, 'road'), '')
    ) AS road_name,
    nullIf(JSONExtractString(raw_message, 'district'), '') AS district,
    nullIf(JSONExtractString(raw_message, 'city'), '') AS city,
    coalesce(
        nullIf(JSONExtractString(raw_message, 'severity'), ''),
        nullIf(JSONExtractString(raw_message, 'level'), ''),
        'warning'
    ) AS severity,
    toFloat64OrNull(
        nullIf(JSONExtractRaw(raw_message, 'avg_speed'), '')
    ) AS avg_speed,
    toFloat64OrNull(
        nullIf(
            JSONExtractRaw(raw_message, 'avg_delay_minutes'),
            ''
        )
    ) AS avg_delay_minutes,
    toInt64OrNull(
        nullIf(
            JSONExtractRaw(raw_message, 'accident_count'),
            ''
        )
    ) AS accident_count,
    coalesce(
        nullIf(JSONExtractString(raw_message, 'message'), ''),
        nullIf(JSONExtractString(raw_message, 'reason'), ''),
        'Congestion alert'
    ) AS message,
    'trafic.congestion_alerts' AS source_topic,
    raw_message
FROM traffic_analytics.congestion_alerts_queue;
CREATE MATERIALIZED VIEW IF NOT EXISTS traffic_analytics.accident_alerts_mv TO traffic_analytics.traffic_alerts AS
SELECT coalesce(
        parseDateTimeBestEffortOrNull(
            nullIf(JSONExtractString(raw_message, 'alert_time'), '')
        ),
        parseDateTimeBestEffortOrNull(
            nullIf(JSONExtractString(raw_message, 'timestamp'), '')
        ),
        parseDateTimeBestEffortOrNull(
            nullIf(JSONExtractString(raw_message, 'event_time'), '')
        ),
        now()
    ) AS alert_time,
    'accident' AS alert_category,
    ifNull(
        coalesce(
            nullIf(JSONExtractString(raw_message, 'road_name'), ''),
            nullIf(JSONExtractString(raw_message, 'road'), '')
        ),
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
    ifNull(
        coalesce(
            nullIf(JSONExtractString(raw_message, 'severity'), ''),
            nullIf(JSONExtractString(raw_message, 'level'), ''),
            'warning'
        ),
        'warning'
    ) AS severity,
    toFloat64OrNull(
        nullIf(JSONExtractRaw(raw_message, 'avg_speed'), '')
    ) AS avg_speed,
    toFloat64OrNull(
        nullIf(
            JSONExtractRaw(raw_message, 'avg_delay_minutes'),
            ''
        )
    ) AS avg_delay_minutes,
    toInt64OrNull(
        nullIf(
            JSONExtractRaw(raw_message, 'accident_count'),
            ''
        )
    ) AS accident_count,
    ifNull(
        coalesce(
            nullIf(JSONExtractString(raw_message, 'message'), ''),
            nullIf(JSONExtractString(raw_message, 'reason'), ''),
            'Congestion alert'
        ),
        'Congestion alert'
    ) AS message,
    'trafic.accident_anomaly_alerts' AS source_topic,
    raw_message
FROM traffic_analytics.accident_alerts_queue;