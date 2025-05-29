FROM bitnami/spark:3.3.4

WORKDIR /app

# Installe curl
USER root
RUN apt-get update && apt-get install -y curl

COPY spark_streaming.py .
COPY spark_analysis.py .

# Télécharge le connecteur GCS
RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/gcs-connector-hadoop3-latest.jar https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar

CMD ["tail", "-f", "/dev/null"]
