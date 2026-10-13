"""Kafka producer module for the ingestion service.

Provides functionality to create a configured Kafka producer with SASL/PLAIN
authentication and to produce serialized IoT sensor records to a Kafka topic.
Records are partitioned by device_id to ensure all messages from the same
device land on the same partition, preserving per-device ordering.

Requirements: FR-1.1, FR-1.3, FR-1.5, NFR-5.3, NFR-5.10
"""

import json
import os
from typing import Any, Dict

from confluent_kafka import Producer, KafkaError, Message

from ingestion_service.logger import get_logger

logger = get_logger("producer")


def delivery_callback(err: KafkaError | None, msg: Message) -> None:
    """Callback invoked on message delivery success or failure.

    Logs delivery errors at ERROR level and successful deliveries at DEBUG
    level with the topic, partition, and offset information.

    Args:
        err: A KafkaError instance if delivery failed, or None on success.
        msg: The Kafka Message object that was delivered (or failed).
    """
    if err is not None:
        logger.error(
            f"Message delivery failed: {err.str()}, "
            f"topic={msg.topic()}, key={msg.key()}"
        )
    else:
        logger.debug(
            f"Message delivered: topic={msg.topic()}, "
            f"partition={msg.partition()}, offset={msg.offset()}"
        )


def create_producer() -> Producer:
    """Create and return a Kafka producer configured with SASL/PLAIN auth.

    Reads connection and authentication parameters from environment variables:
        - KAFKA_BOOTSTRAP_SERVERS: Comma-separated list of broker addresses.
        - INGESTION_KAFKA_USER: SASL username for authentication.
        - INGESTION_KAFKA_PASS: SASL password for authentication.

    Returns:
        A configured confluent_kafka.Producer instance ready to produce
        messages.

    Raises:
        KafkaException: If the producer cannot be created with the given
            configuration.
    """
    bootstrap_servers = os.environ.get(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )
    sasl_username = os.environ.get("INGESTION_KAFKA_USER", "")
    sasl_password = os.environ.get("INGESTION_KAFKA_PASS", "")

    config: Dict[str, Any] = {
        "bootstrap.servers": bootstrap_servers,
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "PLAIN",
        "sasl.username": sasl_username,
        "sasl.password": sasl_password,
        # Durability / delivery guarantees (made explicit rather than relying on
        # library defaults). Idempotence gives effectively-once, in-order
        # production: no duplicate records on retry and preserved per-partition
        # ordering. Enabling it also pins acks=all, retries=INT32_MAX and
        # max.in.flight<=5; acks=all is set explicitly here for clarity so a
        # write is only acknowledged once all in-sync replicas have it.
        "enable.idempotence": True,
        "acks": "all",
    }

    logger.info(
        f"Creating Kafka producer, brokers={bootstrap_servers}"
    )

    producer = Producer(config)
    return producer


def produce_record(
    producer: Producer, topic: str, record: Dict[str, Any]
) -> None:
    """Serialize and produce a single IoT record to the specified Kafka topic.

    The record is serialized as JSON and keyed by its device_id field to
    ensure consistent partitioning per device.

    Args:
        producer: A configured confluent_kafka.Producer instance.
        topic: The Kafka topic to produce the record to.
        record: A dictionary representing an IoT sensor record. Must contain
            a 'device_id' field used as the partition key.
    """
    key = record["device_id"]
    value = json.dumps(record).encode("utf-8")

    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=value,
        callback=delivery_callback,
    )
    producer.poll(0)
