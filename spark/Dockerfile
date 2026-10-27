# syntax=docker/dockerfile:1
# Official Apache Spark image (Scala 2.12, Java 11, Python 3, Ubuntu)
FROM apache/spark:3.5.3-scala2.12-java11-python3-ubuntu

USER root

# Connector libraries for Kafka (source and sink); versions match Spark 3.5.3
ARG MAVEN=https://repo1.maven.org/maven2
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/

# Python driver for the idempotent PostgreSQL/TimescaleDB sink
COPY requirements.txt /tmp/requirements.txt
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3-pip \
 && pip3 install --no-cache-dir -r /tmp/requirements.txt \
 && apt-get purge -y python3-pip && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/* /tmp/requirements.txt

# Checkpoint directory, shared via a named volume between driver and workers
RUN mkdir -p /checkpoints /opt/app && chown -R spark:spark /checkpoints /opt/app

COPY --chown=spark:spark stream_job.py sinks.py /opt/app/

ENV PYTHONUNBUFFERED=1
WORKDIR /opt/app
USER spark
