FROM apache/superset:latest

USER root

RUN uv pip install --python /app/.venv/bin/python --no-cache \
    "clickhouse-connect>=0.13.0" \
    "psycopg2-binary"

USER superset