"""Property-based tests for the Processing Service.

This module contains property tests for the processing pipeline components:
- Property 4: Timestamp Validation Filtering

Uses Hypothesis to verify correctness across a wide range of generated inputs.
"""

from datetime import datetime, timezone, timedelta

import pytest
from hypothesis import given, settings, assume
from hypothesis import strategies as st

from processing_service.validation import validate_timestamp


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# Strategy for valid ISO 8601 datetime strings
valid_iso_timestamps = st.datetimes(
    min_value=datetime(1900, 1, 1),
    max_value=datetime(2100, 12, 31),
    timezones=st.just(timezone.utc) | st.just(None),
).map(lambda dt: dt.isoformat())

# Strategy for valid ISO 8601 date-only strings (also valid ISO 8601)
valid_iso_dates = st.dates(
    min_value=datetime(1900, 1, 1).date(),
    max_value=datetime(2100, 12, 31).date(),
).map(lambda d: d.isoformat())

# Strategy for malformed/unparseable timestamp strings
malformed_timestamps = st.one_of(
    st.text(
        alphabet=st.characters(whitelist_categories=("L",)),
        min_size=1,
        max_size=50,
    ),
    st.from_regex(r"\d{4}/\d{2}/\d{2}", fullmatch=True),
    st.just(""),
    st.just("not-a-date"),
    st.just("2024-13-45"),
    st.just("12:34:56"),
    st.just("yesterday"),
)

# Strategy for non-string types (int, float, list, dict, bool)
non_string_types = st.one_of(
    st.integers(),
    st.floats(allow_nan=False),
    st.booleans(),
    st.lists(st.integers(), max_size=3),
    st.dictionaries(st.text(max_size=5), st.integers(), max_size=3),
)


# ---------------------------------------------------------------------------
# Property 4: Timestamp Validation Filtering
# ---------------------------------------------------------------------------


class TestTimestampValidationFiltering:
    """Property 4: Timestamp Validation Filtering.

    For any Data_Record, if its timestamp field is non-null and parseable as
    ISO 8601, it SHALL pass validation. If the timestamp field is null, missing,
    or not parseable as ISO 8601, the record SHALL be discarded.

    **Validates: Requirements FR-3.2**
    """

    @settings(max_examples=200)
    @given(timestamp_str=valid_iso_timestamps)
    def test_valid_iso8601_timestamps_pass_validation(self, timestamp_str):
        """Valid ISO 8601 datetime strings always pass validation.

        **Validates: Requirements FR-3.2**
        """
        record = {"timestamp": timestamp_str}
        assert validate_timestamp(record) is True

    @settings(max_examples=200)
    @given(date_str=valid_iso_dates)
    def test_valid_iso8601_dates_pass_validation(self, date_str):
        """Valid ISO 8601 date-only strings always pass validation.

        **Validates: Requirements FR-3.2**
        """
        record = {"timestamp": date_str}
        assert validate_timestamp(record) is True

    @settings(max_examples=200)
    @given(
        data=st.fixed_dictionaries(
            {},
            optional={
                "device_id": st.text(max_size=10),
                "temperature": st.floats(allow_nan=False),
                "canton": st.text(max_size=10),
            },
        )
    )
    def test_null_timestamp_is_discarded(self, data):
        """Records with None/null timestamp value are discarded.

        **Validates: Requirements FR-3.2**
        """
        record = {**data, "timestamp": None}
        assert validate_timestamp(record) is False

    @settings(max_examples=200)
    @given(
        data=st.dictionaries(
            keys=st.text(min_size=1, max_size=20).filter(lambda k: k != "timestamp"),
            values=st.text(max_size=20),
            max_size=5,
        )
    )
    def test_missing_timestamp_key_is_discarded(self, data):
        """Records without a 'timestamp' key are discarded.

        **Validates: Requirements FR-3.2**
        """
        # Ensure key is truly absent
        assert "timestamp" not in data
        assert validate_timestamp(data) is False

    @settings(max_examples=200)
    @given(malformed=malformed_timestamps)
    def test_malformed_timestamps_are_discarded(self, malformed):
        """Malformed/unparseable timestamp strings are discarded.

        **Validates: Requirements FR-3.2**
        """
        # Ensure the generated string is truly not parseable
        try:
            datetime.fromisoformat(malformed)
            assume(False)
        except (ValueError, TypeError):
            pass

        record = {"timestamp": malformed}
        assert validate_timestamp(record) is False

    @settings(max_examples=200)
    @given(value=non_string_types)
    def test_non_string_types_are_discarded(self, value):
        """Non-string types (int, float, list, etc.) in timestamp field are discarded.

        **Validates: Requirements FR-3.2**
        """
        record = {"timestamp": value}
        assert validate_timestamp(record) is False


# ---------------------------------------------------------------------------
# Import for Property 8: Row-Level Inclusion Filtering
# ---------------------------------------------------------------------------

from processing_service.filter import apply_row_filter


# ---------------------------------------------------------------------------
# Strategies for Property 8
# ---------------------------------------------------------------------------

# Strategy for field names used in criteria/records
criteria_field_names = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N"), whitelist_characters="_"),
    min_size=1,
    max_size=15,
)

# Strategy for simple criterion values (single match values)
criteria_values = st.one_of(
    st.text(min_size=0, max_size=20),
    st.integers(min_value=-1000, max_value=1000),
    st.floats(min_value=-1000.0, max_value=1000.0, allow_nan=False, allow_infinity=False),
    st.booleans(),
)

# Strategy for criterion: either a single value or a list of acceptable values
criterion_entry = st.one_of(
    criteria_values,
    st.lists(criteria_values, min_size=1, max_size=5),
)

# Strategy for inclusion criteria dictionaries
inclusion_criteria_st = st.dictionaries(
    keys=criteria_field_names,
    values=criterion_entry,
    min_size=1,
    max_size=5,
)

# Strategy for records that contain all criteria fields with matching values
@st.composite
def matching_record(draw):
    """Generate a record that matches all drawn inclusion criteria."""
    criteria = draw(inclusion_criteria_st)
    record = {}
    for field, criterion in criteria.items():
        if isinstance(criterion, list):
            # Pick one of the acceptable values
            value = draw(st.sampled_from(criterion))
        else:
            value = criterion
        record[field] = value
    # Add some extra fields that are not part of criteria
    extra_fields = draw(
        st.dictionaries(
            keys=criteria_field_names.filter(lambda k: k not in criteria),
            values=criteria_values,
            min_size=0,
            max_size=5,
        )
    )
    record.update(extra_fields)
    return record, criteria


@st.composite
def non_matching_record(draw):
    """Generate a record that does NOT match at least one criterion."""
    criteria = draw(inclusion_criteria_st)
    assume(len(criteria) > 0)

    record = {}
    # First, populate all fields with matching values
    for field, criterion in criteria.items():
        if isinstance(criterion, list):
            value = draw(st.sampled_from(criterion))
        else:
            value = criterion
        record[field] = value

    # Now break at least one criterion by choosing a strategy
    break_field = draw(st.sampled_from(sorted(criteria.keys())))
    break_strategy = draw(st.sampled_from(["wrong_value", "missing_field"]))

    if break_strategy == "missing_field":
        del record[break_field]
    else:
        # Generate a value that does NOT match the criterion
        criterion = criteria[break_field]
        if isinstance(criterion, list):
            wrong_value = draw(criteria_values.filter(lambda v: v not in criterion))
        else:
            wrong_value = draw(criteria_values.filter(lambda v: v != criterion))
        record[break_field] = wrong_value

    return record, criteria


# ---------------------------------------------------------------------------
# Property 8: Row-Level Inclusion Filtering
# ---------------------------------------------------------------------------


class TestRowLevelInclusionFiltering:
    """Property 8: Row-Level Inclusion Filtering.

    For any set of Data_Records and any configured inclusion criteria, only
    records matching ALL specified criteria SHALL appear in the processing
    output. Records not matching the criteria SHALL be excluded, and no
    matching record SHALL be dropped.

    **Validates: Requirements NFR-5.9**
    """

    @settings(max_examples=200)
    @given(data=matching_record())
    def test_matching_records_pass_filter(self, data):
        """Records matching all criteria always pass the filter (no matching record dropped).

        **Validates: Requirements NFR-5.9**
        """
        record, criteria = data
        assert apply_row_filter(record, criteria) is True

    @settings(max_examples=200)
    @given(data=non_matching_record())
    def test_non_matching_records_are_excluded(self, data):
        """Records NOT matching at least one criterion are excluded.

        **Validates: Requirements NFR-5.9**
        """
        record, criteria = data
        assert apply_row_filter(record, criteria) is False

    @settings(max_examples=200)
    @given(
        record=st.dictionaries(
            keys=criteria_field_names,
            values=criteria_values,
            min_size=0,
            max_size=10,
        )
    )
    def test_empty_criteria_passes_all_records(self, record):
        """Empty or None criteria means all records pass the filter.

        **Validates: Requirements NFR-5.9**
        """
        assert apply_row_filter(record, None) is True
        assert apply_row_filter(record, {}) is True

    @settings(max_examples=200)
    @given(data=matching_record())
    def test_and_logic_all_criteria_must_match(self, data):
        """AND logic: a record must match ALL criteria, not just some.

        Verifies that if we break any single criterion in a previously matching
        record, it no longer passes the filter.

        **Validates: Requirements NFR-5.9**
        """
        record, criteria = data
        assume(len(criteria) >= 2)

        # Record matches all criteria
        assert apply_row_filter(record, criteria) is True

        # Break one criterion by removing the field
        break_field = sorted(criteria.keys())[0]
        broken_record = {k: v for k, v in record.items() if k != break_field}
        assert apply_row_filter(broken_record, criteria) is False


# ---------------------------------------------------------------------------
# Import for Property 6: Aggregation Mathematical Correctness
# ---------------------------------------------------------------------------

from processing_service.aggregation import compute_aggregation


# ---------------------------------------------------------------------------
# Strategies for Property 6
# ---------------------------------------------------------------------------

# Strategy for numeric values used in aggregation records
aggregation_numeric_values = st.one_of(
    st.integers(min_value=-10000, max_value=10000),
    st.floats(min_value=-10000.0, max_value=10000.0, allow_nan=False, allow_infinity=False),
)

# Strategy for field names used in aggregation
aggregation_field_names = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N"), whitelist_characters="_"),
    min_size=1,
    max_size=15,
)


@st.composite
def aggregation_records_and_fields(draw):
    """Generate a list of records and corresponding numeric field names.

    Produces a non-empty list of records where each record contains the
    specified numeric fields (with numeric values) and possibly extra fields.
    """
    # Draw 1-5 numeric field names
    numeric_fields = draw(
        st.lists(aggregation_field_names, min_size=1, max_size=5, unique=True)
    )

    # Draw 1-50 records, each containing all numeric fields with numeric values
    records = draw(
        st.lists(
            st.fixed_dictionaries(
                {field: aggregation_numeric_values for field in numeric_fields}
            ),
            min_size=1,
            max_size=50,
        )
    )

    return records, numeric_fields


@st.composite
def aggregation_records_with_missing_fields(draw):
    """Generate records where some may be missing numeric fields.

    This tests that the aggregation correctly handles partial data,
    only counting records that have valid numeric values for each field.
    """
    numeric_fields = draw(
        st.lists(aggregation_field_names, min_size=1, max_size=5, unique=True)
    )

    # Each record may or may not contain each numeric field
    records = draw(
        st.lists(
            st.fixed_dictionaries(
                {},
                optional={field: aggregation_numeric_values for field in numeric_fields},
            ),
            min_size=1,
            max_size=50,
        )
    )

    return records, numeric_fields


# ---------------------------------------------------------------------------
# Property 6: Aggregation Mathematical Correctness
# ---------------------------------------------------------------------------


class TestAggregationMathematicalCorrectness:
    """Property 6: Aggregation Mathematical Correctness.

    For any set of Data_Records and configured numeric fields, verify that:
    - count == len(records)
    - sum == mathematical sum of values for each numeric field
    - avg == sum / count (within float tolerance)

    **Validates: Requirements FR-3.4**
    """

    @settings(max_examples=200)
    @given(data=aggregation_records_and_fields())
    def test_count_equals_len_records(self, data):
        """The aggregation count always equals the number of input records.

        **Validates: Requirements FR-3.4**
        """
        records, numeric_fields = data
        result = compute_aggregation(records, numeric_fields)
        assert result["count"] == len(records)

    @settings(max_examples=200)
    @given(data=aggregation_records_and_fields())
    def test_sum_equals_mathematical_sum(self, data):
        """The per-field sum equals the mathematical sum of all numeric values.

        **Validates: Requirements FR-3.4**
        """
        records, numeric_fields = data
        result = compute_aggregation(records, numeric_fields)

        for field in numeric_fields:
            expected_sum = sum(
                r[field] for r in records
                if field in r and isinstance(r[field], (int, float))
            )
            assert result[f"{field}_sum"] == pytest.approx(expected_sum, rel=1e-9, abs=1e-12), (
                f"Field '{field}': expected sum {expected_sum}, got {result[f'{field}_sum']}"
            )

    @settings(max_examples=200)
    @given(data=aggregation_records_and_fields())
    def test_avg_equals_sum_divided_by_count(self, data):
        """The per-field average equals sum / field_count (within float tolerance).

        **Validates: Requirements FR-3.4**
        """
        records, numeric_fields = data
        result = compute_aggregation(records, numeric_fields)

        for field in numeric_fields:
            field_values = [
                r[field] for r in records
                if field in r and isinstance(r[field], (int, float))
            ]
            field_count = len(field_values)
            if field_count > 0:
                expected_avg = sum(field_values) / field_count
                assert result[f"{field}_avg"] == pytest.approx(expected_avg, rel=1e-9, abs=1e-12), (
                    f"Field '{field}': expected avg {expected_avg}, got {result[f'{field}_avg']}"
                )
            else:
                assert result[f"{field}_avg"] is None

    @settings(max_examples=200)
    @given(data=aggregation_records_with_missing_fields())
    def test_field_count_matches_valid_numeric_entries(self, data):
        """The per-field count matches the number of records with valid numeric values.

        **Validates: Requirements FR-3.4**
        """
        records, numeric_fields = data
        result = compute_aggregation(records, numeric_fields)

        assert result["count"] == len(records)

        for field in numeric_fields:
            expected_field_count = sum(
                1 for r in records
                if field in r and isinstance(r[field], (int, float))
            )
            assert result[f"{field}_count"] == expected_field_count, (
                f"Field '{field}': expected count {expected_field_count}, "
                f"got {result[f'{field}_count']}"
            )

    @settings(max_examples=200)
    @given(data=aggregation_records_with_missing_fields())
    def test_mathematical_correctness_with_partial_data(self, data):
        """Combined correctness: count, sum, avg all correct with partial records.

        **Validates: Requirements FR-3.4**
        """
        records, numeric_fields = data
        result = compute_aggregation(records, numeric_fields)

        # Overall count is always len(records)
        assert result["count"] == len(records)

        for field in numeric_fields:
            field_values = [
                r[field] for r in records
                if field in r and isinstance(r[field], (int, float))
            ]
            field_count = len(field_values)

            # Verify field count
            assert result[f"{field}_count"] == field_count

            # Verify sum
            expected_sum = sum(field_values) if field_values else 0
            assert result[f"{field}_sum"] == pytest.approx(expected_sum, rel=1e-9, abs=1e-12)

            # Verify average
            if field_count > 0:
                expected_avg = expected_sum / field_count
                assert result[f"{field}_avg"] == pytest.approx(expected_avg, rel=1e-9, abs=1e-12)
            else:
                assert result[f"{field}_avg"] is None


# ---------------------------------------------------------------------------
# Import for Property 7: Late Arrival Discard
# ---------------------------------------------------------------------------

from processing_service.aggregation import is_late_arrival, SlidingWindowAggregator


# ---------------------------------------------------------------------------
# Strategies for Property 7
# ---------------------------------------------------------------------------

# Strategy for timezone-aware UTC watermark datetimes
watermark_datetimes = st.datetimes(
    min_value=datetime(2000, 1, 2),
    max_value=datetime(2100, 12, 31),
    timezones=st.just(timezone.utc),
)

# Strategy for positive time deltas (used to shift timestamps before/after watermark)
positive_timedeltas = st.timedeltas(
    min_value=timedelta(seconds=1),
    max_value=timedelta(days=365),
)

# Strategy for non-negative time deltas (including zero, for on-time arrivals)
non_negative_timedeltas = st.timedeltas(
    min_value=timedelta(seconds=0),
    max_value=timedelta(days=365),
)


@st.composite
def late_arrival_record(draw):
    """Generate a record with a timestamp strictly before the watermark (late arrival)."""
    watermark = draw(watermark_datetimes)
    delta = draw(positive_timedeltas)
    late_timestamp = watermark - delta
    record = {
        "timestamp": late_timestamp.isoformat(),
        "device_id": draw(st.text(min_size=1, max_size=10)),
        "temperature": draw(st.floats(min_value=-50.0, max_value=50.0, allow_nan=False)),
    }
    return record, watermark


@st.composite
def on_time_record(draw):
    """Generate a record with a timestamp at or after the watermark (on-time arrival)."""
    watermark = draw(watermark_datetimes)
    delta = draw(non_negative_timedeltas)
    on_time_timestamp = watermark + delta
    record = {
        "timestamp": on_time_timestamp.isoformat(),
        "device_id": draw(st.text(min_size=1, max_size=10)),
        "temperature": draw(st.floats(min_value=-50.0, max_value=50.0, allow_nan=False)),
    }
    return record, watermark


@st.composite
def multiple_late_records(draw):
    """Generate a watermark and a list of late-arrival records."""
    watermark = draw(watermark_datetimes)
    count = draw(st.integers(min_value=1, max_value=20))
    records = []
    for _ in range(count):
        delta = draw(positive_timedeltas)
        late_timestamp = watermark - delta
        record = {
            "timestamp": late_timestamp.isoformat(),
            "device_id": draw(st.text(min_size=1, max_size=10)),
            "temperature": draw(st.floats(min_value=-50.0, max_value=50.0, allow_nan=False)),
        }
        records.append(record)
    return records, watermark


# ---------------------------------------------------------------------------
# Property 7: Late Arrival Discard
# ---------------------------------------------------------------------------


class TestLateArrivalDiscard:
    """Property 7: Late Arrival Discard.

    For any Data_Record whose timestamp falls before the current watermark
    (i.e., after the corresponding aggregation window has closed), the record
    SHALL be discarded and the late-arrival counter SHALL be incremented by
    exactly 1.

    **Validates: Requirements FR-3.8**
    """

    @settings(max_examples=200)
    @given(data=late_arrival_record())
    def test_late_records_are_detected(self, data):
        """Records with timestamps before the watermark are identified as late arrivals.

        **Validates: Requirements FR-3.8**
        """
        record, watermark = data
        assert is_late_arrival(record, watermark) is True

    @settings(max_examples=200)
    @given(data=on_time_record())
    def test_on_time_records_are_not_late(self, data):
        """Records with timestamps at or after the watermark are NOT late arrivals.

        **Validates: Requirements FR-3.8**
        """
        record, watermark = data
        assert is_late_arrival(record, watermark) is False

    @settings(max_examples=200)
    @given(data=late_arrival_record())
    def test_late_arrival_counter_increments_by_one(self, data):
        """The late-arrival counter increments by exactly 1 for each discarded record.

        **Validates: Requirements FR-3.8**
        """
        record, watermark = data
        aggregator = SlidingWindowAggregator(
            window_size=300, slide_interval=60
        )
        aggregator.advance_watermark(watermark)

        count_before = aggregator.late_arrival_count
        result = aggregator.add_record(record)

        assert result is False, "Late arrival should be rejected (return False)"
        assert aggregator.late_arrival_count == count_before + 1

    @settings(max_examples=200)
    @given(data=multiple_late_records())
    def test_multiple_late_arrivals_increment_counter_correctly(self, data):
        """Multiple late arrivals increment the counter by exactly the number of late records.

        **Validates: Requirements FR-3.8**
        """
        records, watermark = data
        aggregator = SlidingWindowAggregator(
            window_size=300, slide_interval=60
        )
        aggregator.advance_watermark(watermark)

        assert aggregator.late_arrival_count == 0

        for i, record in enumerate(records, start=1):
            result = aggregator.add_record(record)
            assert result is False, f"Record {i} should be rejected as late arrival"
            assert aggregator.late_arrival_count == i

        assert aggregator.late_arrival_count == len(records)

    @settings(max_examples=200)
    @given(data=on_time_record())
    def test_add_record_returns_true_for_on_time(self, data):
        """SlidingWindowAggregator.add_record returns True for on-time arrivals.

        **Validates: Requirements FR-3.8**
        """
        record, watermark = data
        aggregator = SlidingWindowAggregator(
            window_size=300, slide_interval=60
        )
        aggregator.advance_watermark(watermark)

        count_before = aggregator.late_arrival_count
        result = aggregator.add_record(record)

        assert result is True, "On-time record should be accepted (return True)"
        assert aggregator.late_arrival_count == count_before
