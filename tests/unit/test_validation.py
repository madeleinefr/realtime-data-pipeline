"""Unit tests for the timestamp validation module.

Tests verify that validate_timestamp correctly accepts valid ISO 8601 timestamps
and rejects records with missing, null, or unparseable timestamp values.
"""

import pytest

from processing_service.validation import validate_timestamp


class TestValidateTimestamp:
    """Tests for the validate_timestamp function."""

    def test_valid_iso_8601_with_timezone(self):
        """A full ISO 8601 timestamp with timezone offset is valid."""
        record = {"timestamp": "2024-01-15T10:30:00+00:00"}
        assert validate_timestamp(record) is True

    def test_valid_iso_8601_without_timezone(self):
        """An ISO 8601 timestamp without timezone (naive) is valid."""
        record = {"timestamp": "2024-01-15T10:30:00"}
        assert validate_timestamp(record) is True

    def test_valid_iso_8601_date_only(self):
        """A date-only ISO 8601 string is valid (parseable by fromisoformat)."""
        record = {"timestamp": "2024-01-15"}
        assert validate_timestamp(record) is True

    def test_valid_iso_8601_with_microseconds(self):
        """An ISO 8601 timestamp with microseconds is valid."""
        record = {"timestamp": "2024-01-15T10:30:00.123456+00:00"}
        assert validate_timestamp(record) is True

    def test_missing_timestamp_key(self):
        """A record missing the timestamp key is invalid."""
        record = {"sensor_id": "sensor-01", "value": 42.0}
        assert validate_timestamp(record) is False

    def test_null_timestamp_value(self):
        """A record with a None timestamp value is invalid."""
        record = {"timestamp": None}
        assert validate_timestamp(record) is False

    def test_empty_string_timestamp(self):
        """A record with an empty string timestamp is invalid."""
        record = {"timestamp": ""}
        assert validate_timestamp(record) is False

    def test_unparseable_timestamp_string(self):
        """A record with a non-ISO 8601 string is invalid."""
        record = {"timestamp": "not-a-date"}
        assert validate_timestamp(record) is False

    def test_numeric_timestamp_value(self):
        """A record with a numeric (epoch) timestamp is invalid (not ISO string)."""
        record = {"timestamp": 1705312200}
        assert validate_timestamp(record) is False

    def test_boolean_timestamp_value(self):
        """A record with a boolean timestamp value is invalid."""
        record = {"timestamp": True}
        assert validate_timestamp(record) is False

    def test_empty_record(self):
        """An empty record is invalid (no timestamp key)."""
        record = {}
        assert validate_timestamp(record) is False

    def test_valid_timestamp_with_other_fields(self):
        """A record with a valid timestamp among other fields is valid."""
        record = {
            "sensor_id": "sensor-01",
            "timestamp": "2024-06-10T14:22:33+02:00",
            "value": 23.5,
        }
        assert validate_timestamp(record) is True

    def test_invalid_date_format_slash_separated(self):
        """A slash-separated date string is invalid for ISO 8601."""
        record = {"timestamp": "2024/01/15 10:30:00"}
        assert validate_timestamp(record) is False
