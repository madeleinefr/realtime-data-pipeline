"""Property-based tests for the IoT sensor data generator.

Uses Hypothesis to verify that the generator module produces records with
monotonically non-decreasing timestamps across sequential calls.
"""

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
