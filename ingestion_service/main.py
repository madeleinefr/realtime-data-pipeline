"""Entry point for the ingestion service.

Connects to Kafka with retry logic, generates synthetic IoT sensor records,
and produces them at a configurable rate. Handles connection loss by flushing
logs and exiting non-zero if unable to produce for more than 60 seconds.

Configuration via environment variables:
    - KAFKA_BOOTSTRAP_SERVERS: Broker addresses (default: localhost:9092)
    - KAFKA_TOPIC: Target topic name (default: iot-sensor-data)
    - PRODUCE_RATE: Records per second (default: 1.0)
    - RECORD_LIMIT: Max records to produce, unlimited if unset (default: None)
    - INGESTION_KAFKA_USER: SASL username
    - INGESTION_KAFKA_PASS: SASL password

Requirements: FR-1.1, FR-1.3, FR-1.5, FR-1.6, FR-1.7, NFR-3.4, NFR-6.6, NFR-6.7
"""

import os
import sys
import time
from typing import Optional

from ingestion_service.generator import generate_device_record
from ingestion_service.logger import get_logger
from ingestion_service.producer import create_producer, produce_record
from ingestion_service.retry import retry_connect

logger = get_logger("main")

# Maximum time in seconds without a successful produce before exiting
CONNECTION_LOSS_TIMEOUT: float = 60.0


def get_produce_rate() -> float:
    """Read the PRODUCE_RATE environment variable.

    Returns:
        The configured produce rate in records per second. Defaults to 1.0
        if not set or if the value is invalid.
    """
    raw = os.environ.get("PRODUCE_RATE", "1.0")
    try:
        rate = float(raw)
        if rate <= 0:
            logger.warning(
                f"Invalid PRODUCE_RATE={raw}, using default 1.0"
            )
            return 1.0
        return rate
    except ValueError:
        logger.warning(
            f"Invalid PRODUCE_RATE={raw}, using default 1.0"
        )
        return 1.0


def get_record_limit() -> Optional[int]:
    """Read the RECORD_LIMIT environment variable.

    Returns:
        The maximum number of records to produce, or None if unlimited.
    """
    raw = os.environ.get("RECORD_LIMIT")
    if raw is None or raw.strip() == "":
        return None
    try:
        limit = int(raw)
        if limit <= 0:
            logger.warning(
                f"Invalid RECORD_LIMIT={raw}, running unlimited"
            )
            return None
        return limit
    except ValueError:
        logger.warning(
            f"Invalid RECORD_LIMIT={raw}, running unlimited"
        )
        return None


def run() -> None:
    """Main execution loop for the ingestion service.

    Establishes a Kafka connection with retry logic, then continuously
    generates and produces IoT sensor records at the configured rate.
    If producing fails for longer than 60 seconds, flushes remaining
    messages and exits with a non-zero code.
    """
    topic = os.environ.get("KAFKA_TOPIC", "iot-sensor-data")
    produce_rate = get_produce_rate()
    record_limit = get_record_limit()
    interval = 1.0 / produce_rate

    logger.info(
        f"Starting ingestion service: topic={topic}, "
        f"rate={produce_rate} rec/s, limit={record_limit}"
    )

    # Connect to Kafka with retry logic
    producer = retry_connect(create_producer)
    logger.info("Kafka producer connected successfully")

    records_produced: int = 0
    last_successful_produce: float = time.time()

    while True:
        # Check if record limit has been reached
        if record_limit is not None and records_produced >= record_limit:
            logger.info(
                f"Record limit reached ({record_limit}). "
                f"Flushing and shutting down."
            )
            producer.flush()
            break

        record = generate_device_record()

        try:
            produce_record(producer, topic, record)
            last_successful_produce = time.time()
            records_produced += 1

            if records_produced % 100 == 0:
                logger.info(f"Produced {records_produced} records")

        except Exception as e:
            elapsed = time.time() - last_successful_produce
            logger.error(
                f"Failed to produce record: {e}, "
                f"seconds since last success: {elapsed:.1f}"
            )

            if elapsed > CONNECTION_LOSS_TIMEOUT:
                logger.error(
                    f"Connection lost for >{CONNECTION_LOSS_TIMEOUT}s. "
                    f"Flushing and exiting."
                )
                try:
                    producer.flush(timeout=5)
                except Exception:
                    pass
                sys.exit(1)

        time.sleep(interval)


if __name__ == "__main__":
    run()
