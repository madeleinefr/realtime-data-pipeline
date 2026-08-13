"""Unit tests for the structured JSON logging modules.

Tests verify that both the ingestion_service and processing_service logger
modules produce valid JSON output with required fields, respect the LOG_LEVEL
environment variable, and correctly include the service name.
"""

import json
import logging
import os

import pytest

from ingestion_service.logger import JSONFormatter as IngestionJSONFormatter
from ingestion_service.logger import get_logger as ingestion_get_logger
from processing_service.logger import JSONFormatter as ProcessingJSONFormatter
from processing_service.logger import get_logger as processing_get_logger


@pytest.fixture(autouse=True)
def _cleanup_loggers():
    """Remove test loggers after each test to avoid handler accumulation."""
    yield
    # Clean up any loggers created during tests
    logger_manager = logging.Logger.manager
    loggers_to_remove = [
        name for name in logger_manager.loggerDict if name.startswith("json_logger.")
    ]
    for name in loggers_to_remove:
        logger = logging.getLogger(name)
        logger.handlers.clear()
        del logger_manager.loggerDict[name]


@pytest.fixture
def _clear_log_level_env(monkeypatch):
    """Ensure LOG_LEVEL environment variable is unset before each test."""
    monkeypatch.delenv("LOG_LEVEL", raising=False)


class TestIngestionLogger:
    """Tests for the ingestion_service logger module."""

    def test_log_output_is_valid_json(self, capsys, _clear_log_level_env):
        """Log output must be parseable as valid JSON."""
        logger = ingestion_get_logger("test-ingestion")
        logger.info("test message")

        captured = capsys.readouterr()
        log_entry = json.loads(captured.err.strip())
        assert isinstance(log_entry, dict)

    def test_log_entry_contains_required_fields(self, capsys, _clear_log_level_env):
        """Each log entry must contain timestamp, level, service, and message."""
        logger = ingestion_get_logger("test-ingestion-fields")
        logger.info("hello world")

        captured = capsys.readouterr()
        log_entry = json.loads(captured.err.strip())

        assert "timestamp" in log_entry
        assert "level" in log_entry
        assert "service" in log_entry
        assert "message" in log_entry

    def test_timestamp_is_iso_8601(self, capsys, _clear_log_level_env):
        """Timestamp field must be in ISO 8601 format."""
        logger = ingestion_get_logger("test-ingestion-ts")
        logger.info("timestamp test")

        captured = capsys.readouterr()
        log_entry = json.loads(captured.err.strip())

        # ISO 8601 timestamps contain 'T' separator and timezone info
        timestamp = log_entry["timestamp"]
        assert "T" in timestamp
        # Should end with timezone offset (e.g., +00:00)
        assert "+" in timestamp or "Z" in timestamp

    def test_service_name_appears_in_output(self, capsys, _clear_log_level_env):
        """The service name passed to get_logger must appear in the log entry."""
        logger = ingestion_get_logger("my-ingestion-service")
        logger.info("service name test")

        captured = capsys.readouterr()
        log_entry = json.loads(captured.err.strip())

        assert log_entry["service"] == "my-ingestion-service"

    def test_log_level_info_in_output(self, capsys, _clear_log_level_env):
        """The level field must reflect the log level used."""
        logger = ingestion_get_logger("test-ingestion-level")
        logger.warning("a warning")

        captured = capsys.readouterr()
        log_entry = json.loads(captured.err.strip())

        assert log_entry["level"] == "WARNING"

    def test_log_level_env_variable_respected(
        self, capsys, monkeypatch, _clear_log_level_env
    ):
        """Logger must respect LOG_LEVEL environment variable."""
        monkeypatch.setenv("LOG_LEVEL", "ERROR")
        logger = ingestion_get_logger("test-ingestion-env")

        # INFO should be suppressed when level is ERROR
        logger.info("should not appear")
        captured = capsys.readouterr()
        assert captured.err.strip() == ""

        # ERROR should pass through
        logger.error("should appear")
        captured = capsys.readouterr()
        log_entry = json.loads(captured.err.strip())
        assert log_entry["level"] == "ERROR"

    def test_log_level_defaults_to_info(self, capsys, _clear_log_level_env):
        """Without LOG_LEVEL env var, logger defaults to INFO level."""
        logger = ingestion_get_logger("test-ingestion-default")

        # DEBUG should be suppressed at INFO level
        logger.debug("debug message")
        captured = capsys.readouterr()
        assert captured.err.strip() == ""

        # INFO should pass through
        logger.info("info message")
        captured = capsys.readouterr()
        assert captured.err.strip() != ""

    def test_message_field_contains_log_message(self, capsys, _clear_log_level_env):
        """The message field must contain the actual log message text."""
        logger = ingestion_get_logger("test-ingestion-msg")
        logger.info("specific test message content")

        captured = capsys.readouterr()
        log_entry = json.loads(captured.err.strip())

        assert log_entry["message"] == "specific test message content"


class TestProcessingLogger:
    """Tests for the processing_service logger module."""

    def test_log_output_is_valid_json(self, capsys, _clear_log_level_env):
        """Log output must be parseable as valid JSON."""
        logger = processing_get_logger("test-processing")
        logger.info("test message")

        captured = capsys.readouterr()
        log_entry = json.loads(captured.err.strip())
        assert isinstance(log_entry, dict)

    def test_log_entry_contains_required_fields(self, capsys, _clear_log_level_env):
        """Each log entry must contain timestamp, level, service, and message."""
        logger = processing_get_logger("test-processing-fields")
        logger.info("hello world")

        captured = capsys.readouterr()
        log_entry = json.loads(captured.err.strip())

        assert "timestamp" in log_entry
        assert "level" in log_entry
        assert "service" in log_entry
        assert "message" in log_entry

    def test_service_name_appears_in_output(self, capsys, _clear_log_level_env):
        """The service name passed to get_logger must appear in the log entry."""
        logger = processing_get_logger("my-processing-service")
        logger.info("service name test")

        captured = capsys.readouterr()
        log_entry = json.loads(captured.err.strip())

        assert log_entry["service"] == "my-processing-service"

    def test_log_level_env_variable_respected(
        self, capsys, monkeypatch, _clear_log_level_env
    ):
        """Logger must respect LOG_LEVEL environment variable."""
        monkeypatch.setenv("LOG_LEVEL", "WARNING")
        logger = processing_get_logger("test-processing-env")

        # INFO should be suppressed when level is WARNING
        logger.info("should not appear")
        captured = capsys.readouterr()
        assert captured.err.strip() == ""

        # WARNING should pass through
        logger.warning("should appear")
        captured = capsys.readouterr()
        log_entry = json.loads(captured.err.strip())
        assert log_entry["level"] == "WARNING"

    def test_log_level_defaults_to_info(self, capsys, _clear_log_level_env):
        """Without LOG_LEVEL env var, logger defaults to INFO level."""
        logger = processing_get_logger("test-processing-default")

        # DEBUG should be suppressed at INFO level
        logger.debug("debug message")
        captured = capsys.readouterr()
        assert captured.err.strip() == ""

        # INFO should pass through
        logger.info("info message")
        captured = capsys.readouterr()
        assert captured.err.strip() != ""
