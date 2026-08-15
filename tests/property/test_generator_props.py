"""Property-based tests for the IoT sensor data generator.

Uses Hypothesis to verify that the generator module produces records with
monotonically non-decreasing timestamps across sequential calls, and that
all generated records conform to the expected schema.
"""

import re
import uuid
from datetime import datetime

from hypothesis import given, settings
from hypothesis import strategies as st

import ingestion_service.generator as generator
from ingestion_service.generator import generate_device_record


class TestTimestampMonotonicOrdering:
    """Property 1: Timestamp Monotonic Ordering.

    **Validates: Requirements FR-1.2**

    For any sequence of generated records, each timestamp must be greater than
    or equal to the previous timestamp, ensuring monotonically non-decreasing
    chronological ordering.
    """

    @settings(max_examples=200)
    @given(sequence_length=st.integers(min_value=2, max_value=50))
    def test_timestamps_are_monotonically_non_decreasing(self, sequence_length):
        """**Validates: Requirements FR-1.2**

        For any sequence of generated device records, each record's timestamp
        must be >= the previous record's timestamp.
        """
        # Reset module-level state for test isolation
        generator._last_timestamp = datetime.now(generator.timezone.utc)

        # Generate a sequence of records
        records = [generate_device_record() for _ in range(sequence_length)]

        # Parse timestamps and verify monotonic ordering
        timestamps = [
            datetime.fromisoformat(record["timestamp"]) for record in records
        ]

        for i in range(1, len(timestamps)):
            assert timestamps[i] >= timestamps[i - 1], (
                f"Timestamp at index {i} ({timestamps[i].isoformat()}) is less than "
                f"timestamp at index {i - 1} ({timestamps[i - 1].isoformat()})"
            )


class TestGeneratedRecordSchemaValidity:
    """Property 2: Generated Record Schema Validity.

    **Validates: Requirements FR-1.4**

    Every generated record must contain all required fields with correct types
    and value constraints (UUID, ISO 8601, valid canton code, lat/lon ranges,
    enum values).
    """

    # Valid values for enum fields
    VALID_DEVICE_TYPES = [
        "weather_station",
    ]

    VALID_CANTONS = [
        "ZH", "BE", "LU", "UR", "SZ", "OW", "NW", "GL", "ZG", "FR",
        "SO", "BS", "BL", "SH", "AR", "AI", "SG", "GR", "AG", "TG",
        "TI", "VD", "VS", "NE", "GE", "JU",
    ]

    VALID_STATUSES = ["active", "inactive", "maintenance", "error"]

    REQUIRED_FIELDS = [
        "record_id", "timestamp", "device_id", "device_type", "canton",
        "latitude", "longitude", "temperature", "humidity", "pressure",
        "status",
    ]

    @settings(max_examples=200)
    @given(st.integers(min_value=1, max_value=1))
    def test_generated_record_has_valid_schema(self, _):
        """**Validates: Requirements FR-1.4**

        Each generated record must have all 11 required fields present with
        correct types and values within their specified constraints.
        """
        record = generate_device_record()

        # All 11 required fields must be present
        for field in self.REQUIRED_FIELDS:
            assert field in record, f"Missing required field: {field}"

        # record_id must be a valid UUID4
        parsed_uuid = uuid.UUID(record["record_id"], version=4)
        assert str(parsed_uuid) == record["record_id"]

        # timestamp must be valid ISO 8601
        parsed_ts = datetime.fromisoformat(record["timestamp"])
        assert parsed_ts is not None

        # device_id must match pattern device-\d{3} with number 1-500
        assert re.match(r"^device-\d{3}$", record["device_id"]), (
            f"device_id '{record['device_id']}' does not match pattern device-NNN"
        )
        device_number = int(record["device_id"].split("-")[1])
        assert 1 <= device_number <= 500, (
            f"device_id number {device_number} not in range [1, 500]"
        )

        # device_type must be a valid enum value
        assert record["device_type"] in self.VALID_DEVICE_TYPES, (
            f"Invalid device_type: {record['device_type']}"
        )

        # canton must be a valid Swiss canton code
        assert record["canton"] in self.VALID_CANTONS, (
            f"Invalid canton: {record['canton']}"
        )

        # latitude must be a float in range [45.8, 47.8]
        assert isinstance(record["latitude"], float), (
            f"latitude is not a float: {type(record['latitude'])}"
        )
        assert 45.8 <= record["latitude"] <= 47.8, (
            f"latitude {record['latitude']} not in range [45.8, 47.8]"
        )

        # longitude must be a float in range [5.9, 10.5]
        assert isinstance(record["longitude"], float), (
            f"longitude is not a float: {type(record['longitude'])}"
        )
        assert 5.9 <= record["longitude"] <= 10.5, (
            f"longitude {record['longitude']} not in range [5.9, 10.5]"
        )

        # temperature must be a float in range [-20, 45]
        assert isinstance(record["temperature"], float), (
            f"temperature is not a float: {type(record['temperature'])}"
        )
        assert -20.0 <= record["temperature"] <= 45.0, (
            f"temperature {record['temperature']} not in range [-20, 45]"
        )

        # humidity must be a float in range [0, 100]
        assert isinstance(record["humidity"], float), (
            f"humidity is not a float: {type(record['humidity'])}"
        )
        assert 0.0 <= record["humidity"] <= 100.0, (
            f"humidity {record['humidity']} not in range [0, 100]"
        )

        # pressure must be a float in range [950, 1050]
        assert isinstance(record["pressure"], float), (
            f"pressure is not a float: {type(record['pressure'])}"
        )
        assert 950.0 <= record["pressure"] <= 1050.0, (
            f"pressure {record['pressure']} not in range [950, 1050]"
        )

        # status must be a valid enum value
        assert record["status"] in self.VALID_STATUSES, (
            f"Invalid status: {record['status']}"
        )
