# Dockerfile for the Processing Service
# Runs PySpark Structured Streaming to consume from Kafka, apply
# transformations/aggregations, and publish results back to Kafka.

FROM apache/spark:3.5.1-python3

USER root

# Create non-root user for security
RUN groupadd -r appuser && useradd -r -g appuser -d /app -s /sbin/nologin appuser

WORKDIR /app

# Copy requirements first for Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download spark-sql-kafka connector JAR and dependencies for offline use
RUN mkdir -p /opt/spark/jars && \
    cd /opt/spark/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Copy application code as a package (allows `from processing_service.x import y`)
COPY . /app/processing_service/

# Set PYTHONPATH so 'from processing_service.x import y' works
ENV PYTHONPATH=/app

# Ensure checkpoint directory is writable
RUN mkdir -p /tmp/spark-checkpoints && chmod 777 /tmp/spark-checkpoints

# Switch to non-root user
USER appuser

ENTRYPOINT ["python3", "processing_service/main.py"]
