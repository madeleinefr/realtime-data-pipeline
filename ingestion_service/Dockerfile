# Dockerfile for the Ingestion Service
# Generates synthetic IoT sensor data and publishes to Kafka.

FROM python:3.11-slim

# Create non-root user for security
RUN groupadd -r appuser && useradd -r -g appuser -d /app -s /sbin/nologin appuser

WORKDIR /app

# Copy requirements first for Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code into a package directory
COPY . /app/ingestion_service/

# Set PYTHONPATH so 'from ingestion_service.x import y' works
ENV PYTHONPATH=/app

# Switch to non-root user
USER appuser

ENTRYPOINT ["python", "ingestion_service/main.py"]
