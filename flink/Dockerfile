FROM flink:1.19.1-java17

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && pip3 install --no-cache-dir \
    apache-beam==2.48.0 \
    avro-python3==1.10.2 \
    kafka-python==2.0.2 \
    mlflow==2.21.3 \
    pandas==2.2.3 \
    protobuf==3.20.3 \
    redis==5.2.1 \
    requests==2.32.3 \
    ruamel.yaml==0.18.6 \
    scikit-learn==1.5.2 \
    && rm -rf /var/lib/apt/lists/*

ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar /opt/flink/lib/
RUN chmod 644 /opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar

RUN python3 -m zipfile -e /opt/flink/opt/python/pyflink.zip /opt/flink/opt/python/pyflink-lib \
    && chmod +x /opt/flink/opt/python/pyflink-lib/pyflink/bin/pyflink-udf-runner.sh

ENV PYTHONPATH=/opt/flink/opt/python/pyflink-lib:/opt/flink/opt/python/py4j-0.10.9.7-src.zip:/opt/flink/opt/python/cloudpickle-2.2.0-src.zip

COPY jobs /opt/flink/jobs
RUN chown -R flink:flink /opt/flink/jobs

USER flink
