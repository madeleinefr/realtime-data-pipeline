"""Unit tests for the ingestion_service.producer module."""

import json
from unittest.mock import MagicMock, patch

import pytest

from ingestion_service.producer import (
    create_producer,
    delivery_callback,
    produce_record,
)


class TestDeliveryCallback:
    """Tests for the delivery_callback function."""

    def test_logs_error_on_delivery_failure(self, caplog):
        """delivery_callback logs an error when err is not None."""
        err = MagicMock()
        err.str.return_value = "Broker not available"
        msg = MagicMock()
        msg.topic.return_value = "test-topic"
        msg.key.return_value = b"device-001"

        with patch("ingestion_service.producer.logger") as mock_logger:
            delivery_callback(err, msg)
            mock_logger.error.assert_called_once()
            call_args = mock_logger.error.call_args[0][0]
            assert "delivery failed" in call_args.lower()
            assert "Broker not available" in call_args

    def test_logs_debug_on_successful_delivery(self):
        """delivery_callback logs at debug level on success."""
        msg = MagicMock()
        msg.topic.return_value = "test-topic"
        msg.partition.return_value = 2
        msg.offset.return_value = 42

        with patch("ingestion_service.producer.logger") as mock_logger:
            delivery_callback(None, msg)
            mock_logger.debug.assert_called_once()
            call_args = mock_logger.debug.call_args[0][0]
            assert "test-topic" in call_args
            assert "2" in call_args
            assert "42" in call_args


class TestCreateProducer:
    """Tests for the create_producer function."""

    @patch("ingestion_service.producer.Producer")
    def test_creates_producer_with_sasl_config(self, mock_producer_cls):
        """create_producer passes SASL/PLAIN config to Producer."""
        mock_producer_cls.return_value = MagicMock()

        with patch.dict(
            "os.environ",
            {
                "KAFKA_BOOTSTRAP_SERVERS": "broker1:9092,broker2:9092",
                "INGESTION_KAFKA_USER": "testuser",
                "INGESTION_KAFKA_PASS": "testpass",
            },
        ):
            producer = create_producer()

        config = mock_producer_cls.call_args[0][0]
        assert config["bootstrap.servers"] == "broker1:9092,broker2:9092"
        assert config["security.protocol"] == "SASL_PLAINTEXT"
        assert config["sasl.mechanism"] == "PLAIN"
        assert config["sasl.username"] == "testuser"
        assert config["sasl.password"] == "testpass"
        assert producer is mock_producer_cls.return_value

    @patch("ingestion_service.producer.Producer")
    def test_uses_default_bootstrap_servers(self, mock_producer_cls):
        """create_producer defaults to localhost:9092 when env var is missing."""
        mock_producer_cls.return_value = MagicMock()

        with patch.dict(
            "os.environ",
            {
                "INGESTION_KAFKA_USER": "user",
                "INGESTION_KAFKA_PASS": "pass",
            },
            clear=True,
        ):
            # Ensure KAFKA_BOOTSTRAP_SERVERS is not set
            import os
            os.environ.pop("KAFKA_BOOTSTRAP_SERVERS", None)
            create_producer()

        config = mock_producer_cls.call_args[0][0]
        assert config["bootstrap.servers"] == "localhost:9092"


class TestProduceRecord:
    """Tests for the produce_record function."""

    def test_produces_json_serialized_record_with_device_id_key(self):
        """produce_record serializes record as JSON and keys by device_id."""
        mock_producer = MagicMock()
        record = {
            "record_id": "abc-123",
            "device_id": "device-042",
            "temperature": 23.5,
        }

        produce_record(mock_producer, "iot-topic", record)

        mock_producer.produce.assert_called_once()
        call_kwargs = mock_producer.produce.call_args[1]
        assert call_kwargs["topic"] == "iot-topic"
        assert call_kwargs["key"] == b"device-042"

        # Value should be valid JSON matching the record
        value = json.loads(call_kwargs["value"].decode("utf-8"))
        assert value["device_id"] == "device-042"
        assert value["temperature"] == 23.5
        assert value["record_id"] == "abc-123"

    def test_calls_poll_after_produce(self):
        """produce_record calls poll(0) to trigger delivery callbacks."""
        mock_producer = MagicMock()
        record = {"device_id": "device-001", "data": "test"}

        produce_record(mock_producer, "topic", record)

        mock_producer.poll.assert_called_once_with(0)
