"""Property-based tests for structured JSON logging.

Uses Hypothesis to verify that the logger modules produce valid JSON output
with required fields across a wide range of inputs.
"""

import io
import json
import logging
import sys
from datetime import datetime

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from processing_service.logger import JSONFormatter

# Valid Python logging level names
VALID_LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

# Strategy for service names: non-empty text strings
service_names = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N", "P", "S")),
    min_size=1,
    max_size=50,
)

# Strategy for log messages: non-empty text strings
log_messages = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N", "P", "S", "Z")),
    min_size=1,
    max_size=200,
)

# Strategy for log levels (as integers matching Python logging levels)
log_levels = st.sampled_from(
    [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL]
)


class TestStructuredJSONLogFormat:
    """Property 9: Structured JSON Log Format.

    **Validates: NFR-5.5, NFR-6.2**

    For any log event, output is valid JSON containing timestamp (ISO 8601),
    level (one of DEBUG/INFO/WARNING/ERROR/CRITICAL), service (string),
    and message (string).
    """

    @settings(max_examples=200)
    @given(
        service_name=service_names,
        message=log_messages,
        level=log_levels,
    )
    def test_log_output_is_valid_json_with_required_fields(
        self, service_name, message, level
    ):
        """**Validates: NFR-5.5, NFR-6.2**

        For any service name, message, and log level, the logger output must be
        valid JSON containing timestamp (ISO 8601), level, service, and message.
        """
        # Create a logger with a stream handler writing to a StringIO buffer
        stream = io.StringIO()
        logger = logging.getLogger(f"prop_test.{id(stream)}")
        logger.handlers.clear()
        logger.setLevel(logging.DEBUG)
        logger.propagate = False

        handler = logging.StreamHandler(stream)
        handler.setLevel(logging.DEBUG)
        formatter = JSONFormatter(service_name)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Log the message at the given level
        logger.log(level, message)

        # Get output
        output = stream.getvalue().strip()

        # Property: output must be non-empty
        assert output, f"Expected log output but got nothing for level={level}"

        # Property: output must be valid JSON
        log_entry = json.loads(output)
        assert isinstance(log_entry, dict)

        # Property: JSON must contain all required fields
        assert "timestamp" in log_entry, "Missing 'timestamp' field"
        assert "level" in log_entry, "Missing 'level' field"
        assert "service" in log_entry, "Missing 'service' field"
        assert "message" in log_entry, "Missing 'message' field"

        # Property: timestamp must be ISO 8601 format
        timestamp = log_entry["timestamp"]
        assert isinstance(timestamp, str)
        # Parse to verify it's a valid ISO 8601 timestamp
        parsed_ts = datetime.fromisoformat(timestamp)
        assert parsed_ts is not None

        # Property: level must be a valid Python log level name
        assert log_entry["level"] in VALID_LOG_LEVELS, (
            f"Level '{log_entry['level']}' not in {VALID_LOG_LEVELS}"
        )
        assert log_entry["level"] == logging.getLevelName(level)

        # Property: service must be the provided service name (string)
        assert isinstance(log_entry["service"], str)
        assert log_entry["service"] == service_name

        # Property: message must be the provided message (string)
        assert isinstance(log_entry["message"], str)
        assert log_entry["message"] == message

        # Cleanup
        logger.handlers.clear()
        handler.close()
        stream.close()
