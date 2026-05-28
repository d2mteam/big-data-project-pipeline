FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir fastapi kafka-python requests uvicorn

COPY *.py .

CMD ["python", "sensor_producer.py"]
