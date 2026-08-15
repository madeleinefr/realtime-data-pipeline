"""Timestamp validation module for the processing service.

Provides validation logic for incoming data records, ensuring that timestamp
fields are present and conform to ISO 8601 format. Records with invalid or
missing timestamps are rejected early in the pipeline to maintain data quality
and enable reliable downstream time-based processing.
"""

from datetime import datetime
from typing import Any, Dict


def validate_timestamp(record: Dict[str, Any]) -> bool:
    """Validate that a record contains a parseable ISO 8601 timestamp.

    Checks the "timestamp" field of the given record to verify it is non-null
    and can be parsed as a valid ISO 8601 datetime string. This is used as a
    data quality gate in the processing pipeline.

    Args:
        record: A dictionary-like object expected to contain a "timestamp" key
            with an ISO 8601 formatted datetime string value.

    Returns:
        True if the timestamp field is present, non-null, and parseable as
        ISO 8601. False otherwise (missing key, null value, or unparseable
        string).
    """
    timestamp_value = record.get("timestamp")

    if timestamp_value is None:
        return False

    if not isinstance(timestamp_value, str):
        return False

    try:
        datetime.fromisoformat(timestamp_value)
        return True
    except (ValueError, TypeError):
        return False
