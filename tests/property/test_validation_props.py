"""Property-based tests for timestamp validation.

Uses Hypothesis to verify that validate_timestamp correctly distinguishes
valid ISO 8601 timestamps from invalid or missing values across a wide range
of generated inputs.
"""

from datetime import datetime, timezone, timedelta

import pytest
from hypothesis import given, settings, assume
from hypothesis import strategies as st

from processing_service.validation import validate_timestamp


# Strategy for valid ISO 8601 timestamp strings
valid_timestamps = st.datetimes(
    min_value=datetime(1900, 1, 1),
    max_value=datetime(2100, 12, 31),
    timezones=st.just(timezone.utc) | st.just(None),
).map(lambda dt: dt.isoformat())

# Strategy for valid date-only strings
valid_dates = st.dates(
    min_value=datetime(1900, 1, 1).date(),
    max_value=datetime(2100, 12, 31).date(),
).map(lambda d: d.isoformat())

# Strategy for non-timestamp strings that should fail validation
invalid_timestamp_strings = st.one_of(
    st.text(
        alphabet=st.characters(whitelist_categories=("L",)),
        min_size=1,
        max_size=50,
    ),
    st.from_regex(r"\d{4}/\d{2}/\d{2}", fullmatch=True),
    st.just(""),
)

# Strategy for non-string values that should fail validation
non_string_values = st.one_of(
    st.integers(),
    st.floats(allow_nan=False),
    st.booleans(),
    st.lists(st.integers(), max_size=3),
    st.dictionaries(st.text(max_size=5), st.integers(), max_size=3),
)


class TestTimestampValidationProperties:
    """Property tests for validate_timestamp.

    **Validates: FR-3.2, NFR-6.6, NFR-6.7**

    For any record, validate_timestamp returns True if and only if the record
    has a "timestamp" field with a non-null ISO 8601 parseable string value.
    """

    @settings(max_examples=200)
    @given(timestamp_str=valid_timestamps)
    def test_valid_iso_timestamps_always_pass(self, timestamp_str):
        """**Validates: FR-3.2**

        Any valid ISO 8601 datetime string in the timestamp field must pass
        validation.
        """
        record = {"timestamp": timestamp_str}
        assert validate_timestamp(record) is True

    @settings(max_examples=200)
    @given(date_str=valid_dates)
    def test_valid_iso_dates_always_pass(self, date_str):
        """**Validates: FR-3.2**

        Any valid ISO 8601 date string in the timestamp field must pass
        validation.
        """
        record = {"timestamp": date_str}
        assert validate_timestamp(record) is True

    @settings(max_examples=200)
    @given(invalid_str=invalid_timestamp_strings)
    def test_invalid_strings_always_fail(self, invalid_str):
        """**Validates: NFR-6.6**

        Any non-ISO 8601 string in the timestamp field must fail validation.
        """
        # Ensure we don't accidentally generate something parseable
        try:
            datetime.fromisoformat(invalid_str)
            assume(False)  # skip if it happens to be parseable
        except (ValueError, TypeError):
            pass

        record = {"timestamp": invalid_str}
        assert validate_timestamp(record) is False

    @settings(max_examples=200)
    @given(value=non_string_values)
    def test_non_string_values_always_fail(self, value):
        """**Validates: NFR-6.7**

        Any non-string value in the timestamp field must fail validation.
        """
        record = {"timestamp": value}
        assert validate_timestamp(record) is False

    @settings(max_examples=200)
    @given(
        data=st.dictionaries(
            keys=st.text(min_size=1, max_size=20).filter(lambda k: k != "timestamp"),
            values=st.text(max_size=20),
            max_size=5,
        )
    )
    def test_missing_timestamp_key_always_fails(self, data):
        """**Validates: NFR-6.6**

        Any record without a "timestamp" key must fail validation.
        """
        assert validate_timestamp(data) is False

    @settings(max_examples=200)
    @given(timestamp_str=valid_timestamps)
    def test_return_type_is_always_bool(self, timestamp_str):
        """**Validates: NFR-6.7**

        validate_timestamp must always return a boolean value.
        """
        record = {"timestamp": timestamp_str}
        result = validate_timestamp(record)
        assert isinstance(result, bool)

    @settings(max_examples=200)
    @given(value=non_string_values)
    def test_return_type_is_bool_for_invalid_input(self, value):
        """**Validates: NFR-6.7**

        validate_timestamp must return a boolean even for invalid inputs.
        """
        record = {"timestamp": value}
        result = validate_timestamp(record)
        assert isinstance(result, bool)
        assert result is False
