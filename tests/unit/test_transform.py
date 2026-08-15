"""Unit tests for the data transformation module.

Tests verify that apply_transformations correctly removes excluded fields,
trims whitespace from string values, and normalizes numeric fields using
configured scaling factors.
"""

import pytest

from processing_service.transform import apply_transformations


class TestFieldExclusion:
    """Tests for column-level filtering (field exclusion)."""

    def test_single_field_excluded(self):
        """A field in the exclusion list is removed from the output."""
        record = {"sensor_id": "s1", "secret": "abc", "value": 10}
        result = apply_transformations(record, ["secret"])
        assert "secret" not in result
        assert result == {"sensor_id": "s1", "value": 10}

    def test_multiple_fields_excluded(self):
        """Multiple fields in the exclusion list are all removed."""
        record = {"a": 1, "b": 2, "c": 3, "d": 4}
        result = apply_transformations(record, ["b", "d"])
        assert result == {"a": 1, "c": 3}

    def test_empty_exclusion_list(self):
        """An empty exclusion list leaves all fields intact."""
        record = {"sensor_id": "s1", "value": 42.0}
        result = apply_transformations(record, [])
        assert result == {"sensor_id": "s1", "value": 42.0}

    def test_exclusion_of_nonexistent_field(self):
        """Excluding a field not in the record does not cause an error."""
        record = {"sensor_id": "s1", "value": 10}
        result = apply_transformations(record, ["nonexistent"])
        assert result == {"sensor_id": "s1", "value": 10}

    def test_all_fields_excluded(self):
        """Excluding all fields returns an empty dictionary."""
        record = {"a": 1, "b": 2}
        result = apply_transformations(record, ["a", "b"])
        assert result == {}


class TestWhitespaceTrimming:
    """Tests for whitespace trimming on string fields."""

    def test_leading_whitespace_trimmed(self):
        """Leading whitespace is removed from string values."""
        record = {"name": "   hello"}
        result = apply_transformations(record, [])
        assert result["name"] == "hello"

    def test_trailing_whitespace_trimmed(self):
        """Trailing whitespace is removed from string values."""
        record = {"name": "hello   "}
        result = apply_transformations(record, [])
        assert result["name"] == "hello"

    def test_both_sides_trimmed(self):
        """Whitespace on both sides is removed."""
        record = {"name": "  hello  "}
        result = apply_transformations(record, [])
        assert result["name"] == "hello"

    def test_internal_whitespace_preserved(self):
        """Internal whitespace within a string is not affected."""
        record = {"name": "  hello world  "}
        result = apply_transformations(record, [])
        assert result["name"] == "hello world"

    def test_no_whitespace_unchanged(self):
        """A string without surrounding whitespace is unchanged."""
        record = {"name": "hello"}
        result = apply_transformations(record, [])
        assert result["name"] == "hello"

    def test_non_string_fields_not_trimmed(self):
        """Non-string fields are not affected by trimming logic."""
        record = {"value": 42, "flag": True, "items": [1, 2, 3]}
        result = apply_transformations(record, [])
        assert result == {"value": 42, "flag": True, "items": [1, 2, 3]}


class TestNumericNormalization:
    """Tests for numeric field normalization via scaling factors."""

    def test_integer_normalization(self):
        """An integer field is multiplied by the scaling factor."""
        record = {"voltage": 1000}
        config = {"voltage": 0.001}
        result = apply_transformations(record, [], normalization_config=config)
        assert result["voltage"] == pytest.approx(1.0)

    def test_float_normalization(self):
        """A float field is multiplied by the scaling factor."""
        record = {"temperature": 98.6}
        config = {"temperature": 0.5}
        result = apply_transformations(record, [], normalization_config=config)
        assert result["temperature"] == pytest.approx(49.3)

    def test_field_not_in_config_unchanged(self):
        """A numeric field not in the normalization config is unchanged."""
        record = {"voltage": 1000, "current": 5}
        config = {"voltage": 0.001}
        result = apply_transformations(record, [], normalization_config=config)
        assert result["voltage"] == pytest.approx(1.0)
        assert result["current"] == 5

    def test_empty_normalization_config(self):
        """With an empty normalization config, numeric fields are unchanged."""
        record = {"value": 100}
        result = apply_transformations(record, [], normalization_config={})
        assert result["value"] == 100

    def test_none_normalization_config_uses_default(self):
        """Passing None for normalization_config uses the module default."""
        record = {"value": 100}
        result = apply_transformations(record, [])
        assert result["value"] == 100

    def test_scaling_factor_of_one(self):
        """A scaling factor of 1.0 leaves the value unchanged."""
        record = {"value": 42.5}
        config = {"value": 1.0}
        result = apply_transformations(record, [], normalization_config=config)
        assert result["value"] == pytest.approx(42.5)


class TestCombinedTransformations:
    """Tests verifying that all transformations work together."""

    def test_all_transformations_applied(self):
        """Exclusion, trimming, and normalization are all applied."""
        record = {
            "sensor_id": "  sensor-01  ",
            "secret_key": "should-be-removed",
            "voltage": 5000,
            "timestamp": "2024-01-15T10:30:00Z",
        }
        config = {"voltage": 0.001}
        result = apply_transformations(
            record, ["secret_key"], normalization_config=config
        )
        assert "secret_key" not in result
        assert result["sensor_id"] == "sensor-01"
        assert result["voltage"] == pytest.approx(5.0)
        assert result["timestamp"] == "2024-01-15T10:30:00Z"

    def test_original_record_not_mutated(self):
        """The input record is not modified by the transformation."""
        record = {"name": "  hello  ", "value": 100, "secret": "x"}
        config = {"value": 0.5}
        original_copy = record.copy()
        apply_transformations(record, ["secret"], normalization_config=config)
        assert record == original_copy

    def test_empty_record(self):
        """An empty record produces an empty output."""
        result = apply_transformations({}, ["anything"])
        assert result == {}
