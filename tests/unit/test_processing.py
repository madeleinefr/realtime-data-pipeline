"""Unit tests for the Processing_Service pipeline components.

Covers example-based (non-property) unit tests for the processing service's
core building blocks:

- Timestamp validation (validation.validate_timestamp) - FR-3.2
- Row-level inclusion filtering (filter.apply_row_filter) - NFR-5.9
- Aggregation helpers and sliding-window logic
  (aggregation.compute_aggregation, group_by_canton, is_late_arrival,
  SlidingWindowAggregator) - FR-3.4, FR-3.6, FR-3.8

These example-based tests complement the property-based tests in
tests/property/test_processing_props.py by pinning down specific
transformation examples, window boundary calculations, and error handling
behavior for malformed records.

Requirements: FR-3.2, FR-3.3, FR-3.4, FR-3.6, FR-3.8, NFR-5.8, NFR-5.9
"""

from datetime import datetime, timedelta, timezone

import pytest

from processing_service.aggregation import (
    SlidingWindowAggregator,
    compute_aggregation,
    group_by_canton,
    is_late_arrival,
)
from processing_service.filter import apply_row_filter
from processing_service.validation import validate_timestamp


class TestTimestampValidationExamples:
    """Example-based tests for validate_timestamp (FR-3.2)."""

    @pytest.mark.parametrize(
        "timestamp",
        [
            "2024-01-15T10:30:00",
            "2024-01-15T10:30:00+00:00",
            "2024-01-15T10:30:00.123456",
            "2024-01-15T10:30:00.123456+02:00",
            "2024-12-31T23:59:59",
            "2024-01-15",  # date-only is valid ISO 8601 for fromisoformat
        ],
    )
    def test_valid_iso_timestamps_pass(self, timestamp):
        """Records with parseable ISO 8601 timestamps pass validation."""
        assert validate_timestamp({"timestamp": timestamp}) is True

    @pytest.mark.parametrize(
        "timestamp",
        [
            "not-a-timestamp",
            "2024/01/15 10:30:00",  # wrong separators
            "15-01-2024",  # wrong order
            "10:30:00",  # time-only, not a full ISO datetime/date
            "",  # empty string
            "2024-13-01T00:00:00",  # invalid month
            "2024-01-32T00:00:00",  # invalid day
        ],
    )
    def test_invalid_timestamp_strings_fail(self, timestamp):
        """Records with unparseable timestamp strings fail validation."""
        assert validate_timestamp({"timestamp": timestamp}) is False

    def test_missing_timestamp_field_fails(self):
        """A record without a timestamp field fails validation."""
        assert validate_timestamp({"canton": "ZH", "temperature": 21.0}) is False

    def test_null_timestamp_fails(self):
        """A record with a null timestamp fails validation."""
        assert validate_timestamp({"timestamp": None}) is False

    def test_non_string_timestamp_fails(self):
        """A record with a non-string timestamp value fails validation."""
        assert validate_timestamp({"timestamp": 1705315800}) is False
        assert validate_timestamp({"timestamp": 20240115.5}) is False
        assert validate_timestamp({"timestamp": ["2024-01-15T10:30:00"]}) is False

    def test_empty_record_fails(self):
        """An empty record fails validation (no timestamp present)."""
        assert validate_timestamp({}) is False


class TestRowFilteringDatasets:
    """Row/column filtering with specific datasets (NFR-5.9)."""

    @pytest.fixture
    def dataset(self):
        """A small fixed dataset of records used across filtering tests."""
        return [
            {"canton": "ZH", "device_id": "d1", "temperature": 20.0},
            {"canton": "BE", "device_id": "d2", "temperature": 18.0},
            {"canton": "GE", "device_id": "d3", "temperature": 22.0},
            {"canton": "ZH", "device_id": "d4", "temperature": 19.0},
            {"canton": "TI", "device_id": "d5", "temperature": 25.0},
        ]

    def test_single_criterion_selects_matching_subset(self, dataset):
        """Filtering by a single canton keeps only matching records."""
        criteria = {"canton": "ZH"}
        kept = [r for r in dataset if apply_row_filter(r, criteria)]
        assert [r["device_id"] for r in kept] == ["d1", "d4"]

    def test_list_criterion_selects_union_of_values(self, dataset):
        """A list criterion keeps records matching any listed value."""
        criteria = {"canton": ["BE", "GE"]}
        kept = [r for r in dataset if apply_row_filter(r, criteria)]
        assert [r["device_id"] for r in kept] == ["d2", "d3"]

    def test_multiple_criteria_use_and_logic(self, dataset):
        """Multiple criteria must all match (AND logic)."""
        criteria = {"canton": "ZH", "device_id": "d4"}
        kept = [r for r in dataset if apply_row_filter(r, criteria)]
        assert [r["device_id"] for r in kept] == ["d4"]

    def test_no_criteria_keeps_full_dataset(self, dataset):
        """With no criteria, every record passes."""
        kept = [r for r in dataset if apply_row_filter(r, None)]
        assert len(kept) == len(dataset)

    def test_unmatched_criteria_yields_empty(self, dataset):
        """Criteria that match nothing yield an empty result set."""
        criteria = {"canton": "VD"}
        kept = [r for r in dataset if apply_row_filter(r, criteria)]
        assert kept == []

    def test_record_missing_criterion_field_excluded(self):
        """A record missing a required criterion field is excluded."""
        record = {"device_id": "d99", "temperature": 30.0}
        assert apply_row_filter(record, {"canton": "ZH"}) is False


class TestAggregationExamples:
    """Aggregation math on specific datasets (FR-3.4)."""

    def test_count_sum_avg_for_single_field(self):
        """compute_aggregation returns correct count, sum, and average."""
        records = [
            {"temperature": 10.0},
            {"temperature": 20.0},
            {"temperature": 30.0},
        ]
        result = compute_aggregation(records, ["temperature"])
        assert result["count"] == 3
        assert result["temperature_sum"] == pytest.approx(60.0)
        assert result["temperature_avg"] == pytest.approx(20.0)
        assert result["temperature_count"] == 3

    def test_multiple_numeric_fields(self):
        """Aggregation is computed independently per numeric field."""
        records = [
            {"temperature": 15.0, "humidity": 40.0},
            {"temperature": 25.0, "humidity": 60.0},
        ]
        result = compute_aggregation(records, ["temperature", "humidity"])
        assert result["temperature_sum"] == pytest.approx(40.0)
        assert result["temperature_avg"] == pytest.approx(20.0)
        assert result["humidity_sum"] == pytest.approx(100.0)
        assert result["humidity_avg"] == pytest.approx(50.0)

    def test_empty_records_yield_zero_count_and_none_avg(self):
        """No records means count 0, sum 0, and average None."""
        result = compute_aggregation([], ["temperature"])
        assert result["count"] == 0
        assert result["temperature_sum"] == 0
        assert result["temperature_avg"] is None
        assert result["temperature_count"] == 0

    def test_group_by_canton_partitions_records(self):
        """Records are grouped correctly by their canton field."""
        records = [
            {"canton": "ZH", "temperature": 10.0},
            {"canton": "BE", "temperature": 20.0},
            {"canton": "ZH", "temperature": 30.0},
        ]
        groups = group_by_canton(records)
        assert set(groups.keys()) == {"ZH", "BE"}
        assert len(groups["ZH"]) == 2
        assert len(groups["BE"]) == 1

    def test_group_by_canton_missing_field_uses_empty_key(self):
        """Records without a canton field are grouped under the empty string."""
        records = [{"temperature": 10.0}, {"canton": "ZH", "temperature": 20.0}]
        groups = group_by_canton(records)
        assert "" in groups
        assert len(groups[""]) == 1


class TestMalformedRecordHandling:
    """Error handling for malformed records (FR-3.6)."""

    def test_aggregation_skips_non_numeric_values(self):
        """Non-numeric and missing values are skipped, not raised on."""
        records = [
            {"temperature": 10.0},
            {"temperature": "N/A"},  # malformed: string where number expected
            {"temperature": None},  # malformed: null value
            {},  # malformed: missing field
            {"temperature": 30.0},
        ]
        result = compute_aggregation(records, ["temperature"])
        # count is total records; per-field count only valid numeric values
        assert result["count"] == 5
        assert result["temperature_count"] == 2
        assert result["temperature_sum"] == pytest.approx(40.0)
        assert result["temperature_avg"] == pytest.approx(20.0)

    def test_is_late_arrival_handles_missing_timestamp(self):
        """A record with no timestamp is not treated as a late arrival."""
        watermark = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        assert is_late_arrival({"canton": "ZH"}, watermark) is False

    def test_is_late_arrival_handles_unparseable_timestamp(self):
        """A record with a malformed timestamp is not treated as late."""
        watermark = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        assert is_late_arrival({"timestamp": "garbage"}, watermark) is False

    def test_window_aggregation_ignores_malformed_timestamps(self):
        """Records with bad/missing timestamps are excluded from windows."""
        agg = SlidingWindowAggregator(
            window_size=60, slide_interval=10, numeric_fields=["temperature"]
        )
        start = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        end = start + timedelta(seconds=60)
        agg.add_record(
            {"timestamp": "2024-01-15T10:00:30+00:00", "canton": "ZH", "temperature": 20.0}
        )
        agg.add_record({"timestamp": "not-a-timestamp", "canton": "ZH", "temperature": 99.0})
        agg.add_record({"canton": "ZH", "temperature": 99.0})  # missing timestamp

        result = agg.compute_window_aggregation(start, end)
        assert "ZH" in result
        assert result["ZH"]["count"] == 1
        assert result["ZH"]["temperature_sum"] == pytest.approx(20.0)


class TestSlidingWindowBoundaries:
    """Window boundary calculations and configuration (FR-3.4)."""

    def test_window_boundaries_span_window_size(self):
        """A single window spans exactly window_size seconds ending at ref."""
        agg = SlidingWindowAggregator(window_size=300, slide_interval=60)
        ref = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        windows = agg.get_windows(ref)
        assert len(windows) == 1
        window_start, window_end = windows[0]
        assert window_end == ref
        assert window_end - window_start == timedelta(seconds=300)

    def test_get_windows_makes_naive_reference_utc(self):
        """A naive reference time is interpreted as UTC in window boundaries."""
        agg = SlidingWindowAggregator(window_size=60, slide_interval=10)
        naive_ref = datetime(2024, 1, 15, 12, 0, 0)
        window_start, window_end = agg.get_windows(naive_ref)[0]
        assert window_end.tzinfo is not None
        assert window_start.tzinfo is not None
        assert window_end - window_start == timedelta(seconds=60)

    def test_window_includes_start_excludes_end(self):
        """Window membership is [start, end): start inclusive, end exclusive."""
        agg = SlidingWindowAggregator(
            window_size=60, slide_interval=10, numeric_fields=["v"]
        )
        start = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        end = start + timedelta(seconds=60)
        # exactly at start -> included
        agg.add_record({"timestamp": start.isoformat(), "canton": "ZH", "v": 1.0})
        # exactly at end -> excluded
        agg.add_record({"timestamp": end.isoformat(), "canton": "ZH", "v": 2.0})
        # inside -> included
        agg.add_record(
            {"timestamp": (start + timedelta(seconds=30)).isoformat(), "canton": "ZH", "v": 3.0}
        )
        result = agg.compute_window_aggregation(start, end)
        assert result["ZH"]["count"] == 2
        assert result["ZH"]["v_sum"] == pytest.approx(4.0)

    @pytest.mark.parametrize("window_size", [59, 3601, 0, -1])
    def test_invalid_window_size_raises(self, window_size):
        """Window size outside [60, 3600] seconds is rejected."""
        with pytest.raises(ValueError, match="window_size"):
            SlidingWindowAggregator(window_size=window_size, slide_interval=1)

    @pytest.mark.parametrize("slide_interval", [0, -5, 61])
    def test_invalid_slide_interval_raises(self, slide_interval):
        """Slide interval outside [1, window_size] is rejected."""
        with pytest.raises(ValueError, match="slide_interval"):
            SlidingWindowAggregator(window_size=60, slide_interval=slide_interval)

    def test_boundary_window_and_slide_values_accepted(self):
        """Boundary-valid config values are accepted without error."""
        # min window / min slide
        SlidingWindowAggregator(window_size=60, slide_interval=1)
        # max window / slide equal to window
        SlidingWindowAggregator(window_size=3600, slide_interval=3600)


class TestLateArrivalCounting:
    """Late-arrival discard and counter behavior (FR-3.8)."""

    def test_late_record_discarded_and_counter_incremented(self):
        """A record before the watermark is discarded and counted once."""
        agg = SlidingWindowAggregator(window_size=60, slide_interval=10)
        watermark = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        agg.advance_watermark(watermark)

        late_record = {
            "timestamp": (watermark - timedelta(seconds=5)).isoformat(),
            "canton": "ZH",
            "temperature": 20.0,
        }
        accepted = agg.add_record(late_record)
        assert accepted is False
        assert agg.late_arrival_count == 1

    def test_on_time_record_accepted_not_counted(self):
        """A record at/after the watermark is accepted and not counted late."""
        agg = SlidingWindowAggregator(window_size=60, slide_interval=10)
        watermark = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        agg.advance_watermark(watermark)

        on_time = {
            "timestamp": (watermark + timedelta(seconds=5)).isoformat(),
            "canton": "ZH",
            "temperature": 20.0,
        }
        accepted = agg.add_record(on_time)
        assert accepted is True
        assert agg.late_arrival_count == 0

    def test_multiple_late_records_increment_by_one_each(self):
        """Each late record increments the counter by exactly 1."""
        agg = SlidingWindowAggregator(window_size=60, slide_interval=10)
        watermark = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        agg.advance_watermark(watermark)

        for i in range(3):
            agg.add_record(
                {
                    "timestamp": (watermark - timedelta(seconds=i + 1)).isoformat(),
                    "canton": "ZH",
                    "temperature": 20.0,
                }
            )
        assert agg.late_arrival_count == 3

    def test_watermark_only_advances_forward(self):
        """Advancing to an earlier watermark is ignored."""
        agg = SlidingWindowAggregator(window_size=60, slide_interval=10)
        later = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        earlier = later - timedelta(minutes=5)
        agg.advance_watermark(later)
        agg.advance_watermark(earlier)
        assert agg.watermark == later

    def test_reset_late_arrival_count_returns_previous(self):
        """Resetting the counter returns the previous value and zeroes it."""
        agg = SlidingWindowAggregator(window_size=60, slide_interval=10)
        agg.advance_watermark(datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc))
        agg.add_record(
            {"timestamp": "2024-01-15T09:59:00+00:00", "canton": "ZH", "temperature": 1.0}
        )
        previous = agg.reset_late_arrival_count()
        assert previous == 1
        assert agg.late_arrival_count == 0

    def test_no_watermark_accepts_all_records(self):
        """With no watermark set, records are never considered late."""
        agg = SlidingWindowAggregator(window_size=60, slide_interval=10)
        accepted = agg.add_record(
            {"timestamp": "2000-01-01T00:00:00+00:00", "canton": "ZH", "temperature": 5.0}
        )
        assert accepted is True
        assert agg.late_arrival_count == 0
