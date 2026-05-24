FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir requests kafka-python

COPY producer.py .

CMD ["python", "producer.py"]
