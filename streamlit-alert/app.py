import os

import pandas as pd
import streamlit as st
import clickhouse_connect
from streamlit_autorefresh import st_autorefresh


st.set_page_config(
    page_title="Traffic Road Status & Alerts",
    layout="wide",
)


CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "traffic_analytics")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")


@st.cache_resource
def get_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


def query_df(sql: str) -> pd.DataFrame:
    client = get_client()
    return client.query_df(sql)


@st.cache_data(ttl=5)
def load_road_status() -> pd.DataFrame:
    sql = """
    SELECT
        city,
        district,
        road_name,
        latitude,
        longitude,
        avg_speed,
        vehicle_count,
        avg_delay_minutes,
        accident_count,
        weather_condition,
        is_rain,
        last_event_time,
        multiIf(
            accident_count >= 1, 'ACCIDENT',
            avg_speed < 10 OR avg_delay_minutes >= 10, 'CONGESTED',
            avg_speed < 20 OR avg_delay_minutes >= 5, 'SLOW',
            'NORMAL'
        ) AS road_status
    FROM
    (
        SELECT
            city,
            district,
            road_name,
            argMax(latitude, event_time) AS latitude,
            argMax(longitude, event_time) AS longitude,
            argMax(avg_speed, event_time) AS avg_speed,
            argMax(vehicle_count, event_time) AS vehicle_count,
            argMax(avg_delay_minutes, event_time) AS avg_delay_minutes,
            argMax(accident_count, event_time) AS accident_count,
            argMax(weather_condition, event_time) AS weather_condition,
            argMax(is_rain, event_time) AS is_rain,
            max(event_time) AS last_event_time
        FROM traffic_analytics.enriched_events
        GROUP BY
            city,
            district,
            road_name
    )
    ORDER BY
        avg_delay_minutes DESC,
        avg_speed ASC
    LIMIT 500
    """
    return query_df(sql)


@st.cache_data(ttl=5)
def load_alerts() -> pd.DataFrame:
    sql = """
    SELECT
        alert_time,
        alert_category,
        road_name,
        district,
        city,
        severity,
        avg_speed,
        avg_delay_minutes,
        accident_count,
        message,
        source_topic
    FROM traffic_analytics.traffic_alerts
    ORDER BY alert_time DESC
    LIMIT 300
    """
    try:
        return query_df(sql)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=5)
def load_predictions() -> pd.DataFrame:
    sql = """
    SELECT
        prediction_time,
        road_name,
        district,
        city,
        forecast_horizon_minutes,
        current_cluster,
        predicted_cluster,
        predicted_traffic_status,
        expected_avg_speed,
        expected_avg_delay_minutes,
        model_status
    FROM
    (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY city, district, road_name
                ORDER BY prediction_time DESC
            ) AS rn
        FROM traffic_analytics.traffic_predictions
    )
    WHERE rn = 1
    ORDER BY prediction_time DESC
    LIMIT 500
    """
    try:
        return query_df(sql)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=5)
def load_summary() -> pd.DataFrame:
    sql = """
    SELECT
        count() AS total_events,
        countDistinct(road_name) AS total_roads,
        avg(avg_speed) AS avg_speed,
        avg(avg_delay_minutes) AS avg_delay,
        max(event_time) AS latest_event_time
    FROM traffic_analytics.enriched_events
    """
    return query_df(sql)


st.title("🚦 Traffic Road Status & Accident Alerts")

with st.sidebar:
    st.header("Settings")
    auto_refresh = st.toggle("Auto refresh", value=True)
    refresh_seconds = st.slider("Refresh interval seconds", 3, 60, 10)

    if auto_refresh:
        st_autorefresh(interval=refresh_seconds * 1000, key="traffic_refresh")

    st.caption("Data source: ClickHouse")


try:
    summary_df = load_summary()
    road_df = load_road_status()
    alert_df = load_alerts()
    prediction_df = load_predictions()
except Exception as exc:
    st.error("Cannot load data from ClickHouse.")
    st.exception(exc)
    st.stop()


if summary_df.empty:
    st.warning("No enriched traffic data found.")
    st.stop()


summary = summary_df.iloc[0]

total_roads = int(summary["total_roads"] or 0)
avg_speed = float(summary["avg_speed"] or 0)
avg_delay = float(summary["avg_delay"] or 0)
latest_event_time = summary["latest_event_time"]

congested_roads = 0
slow_roads = 0
accident_roads = 0

if not road_df.empty:
    congested_roads = int((road_df["road_status"] == "CONGESTED").sum())
    slow_roads = int((road_df["road_status"] == "SLOW").sum())
    accident_roads = int((road_df["road_status"] == "ACCIDENT").sum())

if not road_df.empty and not prediction_df.empty:
    road_df = road_df.merge(
        prediction_df,
        on=["city", "district", "road_name"],
        how="left",
    )
else:
    road_df["predicted_traffic_status"] = None
    road_df["expected_avg_speed"] = None
    road_df["expected_avg_delay_minutes"] = None
    road_df["model_status"] = None
    road_df["prediction_time"] = None

recent_alerts = 0
accident_alerts = 0
congestion_alerts = 0



if not alert_df.empty:
    recent_alerts = len(alert_df)
    accident_alerts = int((alert_df["alert_category"] == "accident").sum())
    congestion_alerts = int((alert_df["alert_category"] == "congestion").sum())


col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric("Total roads", total_roads)
col2.metric("Avg speed", f"{avg_speed:.2f}")
col3.metric("Avg delay", f"{avg_delay:.2f} min")
col4.metric("Problem roads", congested_roads + slow_roads + accident_roads)
col5.metric("Recent alerts", recent_alerts)

if not prediction_df.empty:
    loaded_predictions = int((prediction_df["model_status"] == "loaded").sum())
else:
    loaded_predictions = 0

col6.metric("30m predictions", loaded_predictions)

st.caption(f"Latest event time: {latest_event_time}")


tab_status, tab_predictions, tab_alerts, tab_map, tab_raw = st.tabs(
    [
        "Road status",
        "30-min prediction",
        "Alerts",
        "Map",
        "Raw data",
    ]
)


with tab_status:
    st.subheader("Latest road status")

    if road_df.empty:
        st.info("No road status data.")
    else:
        districts = sorted(road_df["district"].dropna().unique().tolist())
        statuses = sorted(road_df["road_status"].dropna().unique().tolist())

        filter_col1, filter_col2 = st.columns(2)

        with filter_col1:
            selected_districts = st.multiselect(
                "District",
                districts,
                default=districts,
            )

        with filter_col2:
            selected_statuses = st.multiselect(
                "Road status",
                statuses,
                default=statuses,
            )

        filtered = road_df[
            road_df["district"].isin(selected_districts)
            & road_df["road_status"].isin(selected_statuses)
        ]

        st.dataframe(
            filtered[
                [
                    "road_status",
                    "predicted_traffic_status",
                    "road_name",
                    "district",
                    "city",
                    "avg_speed",
                    "expected_avg_speed",
                    "avg_delay_minutes",
                    "expected_avg_delay_minutes",
                    "vehicle_count",
                    "accident_count",
                    "weather_condition",
                    "model_status",
                    "last_event_time",
                    "prediction_time",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

        st.subheader("Top delayed roads")
        top_delay = filtered.head(20)[
            ["road_name", "district", "avg_delay_minutes"]
        ].set_index("road_name")

        if not top_delay.empty:
            st.bar_chart(top_delay["avg_delay_minutes"])

with tab_predictions:
    st.subheader("Predicted road status after 30 minutes")

    if prediction_df.empty:
        st.info("No prediction data found yet.")
        st.caption(
            "Make sure flink-predict-job-submitter is running and model is available in MLflow."
        )
    else:
        p1, p2, p3 = st.columns(3)

        loaded_count = int((prediction_df["model_status"] == "loaded").sum())
        unavailable_count = int((prediction_df["model_status"] == "unavailable").sum())

        p1.metric("Prediction rows", len(prediction_df))
        p2.metric("Loaded model predictions", loaded_count)
        p3.metric("Unavailable model rows", unavailable_count)

        st.dataframe(
            prediction_df[
                [
                    "prediction_time",
                    "road_name",
                    "district",
                    "city",
                    "predicted_traffic_status",
                    "expected_avg_speed",
                    "expected_avg_delay_minutes",
                    "current_cluster",
                    "predicted_cluster",
                    "model_status",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

        if "predicted_traffic_status" in prediction_df.columns:
            status_counts = (
                prediction_df["predicted_traffic_status"]
                .fillna("unknown")
                .replace("", "unknown")
                .value_counts()
                .reset_index()
            )
            status_counts.columns = ["predicted_status", "count"]
            st.subheader("Predicted status distribution")
            st.bar_chart(status_counts.set_index("predicted_status")["count"])


with tab_alerts:
    st.subheader("Traffic alerts")

    a1, a2, a3 = st.columns(3)
    a1.metric("All alerts", recent_alerts)
    a2.metric("Congestion alerts", congestion_alerts)
    a3.metric("Accident alerts", accident_alerts)

    if alert_df.empty:
        st.info("No alerts have been consumed yet.")
        st.caption(
            "Trigger anomaly events or wait for Flink alert jobs to produce messages."
        )
    else:
        st.dataframe(
            alert_df,
            use_container_width=True,
            hide_index=True,
        )


with tab_map:
    st.subheader("Road points map")

    if road_df.empty:
        st.info("No location data.")
    else:
        map_df = road_df.dropna(subset=["latitude", "longitude"]).copy()

        if map_df.empty:
            st.info("No latitude/longitude data found.")
        else:
            map_df = map_df.rename(
                columns={
                    "latitude": "lat",
                    "longitude": "lon",
                }
            )

            st.map(
                map_df[
                    [
                        "lat",
                        "lon",
                        "road_name",
                        "district",
                        "road_status",
                    ]
                ]
            )


with tab_raw:
    st.subheader("Raw road status dataframe")
    st.dataframe(road_df, use_container_width=True, hide_index=True)

    st.subheader("Raw alerts dataframe")
    st.dataframe(alert_df, use_container_width=True, hide_index=True)