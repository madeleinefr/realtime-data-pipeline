"""Unit tests for the row-level filtering module.

Tests verify that apply_row_filter correctly passes records matching all
inclusion criteria and rejects records that fail any criterion. Also tests
the load_inclusion_criteria helper for environment variable parsing.
"""

import json
import os

import pytest

from processing_service.filter import apply_row_filter, load_inclusion_criteria


class TestApplyRowFilterNoCriteria:
    """Tests for behavior when no criteria are specified."""

    def test_none_criteria_passes_all(self):
        """A None criteria allows all records through."""
        record = {"sensor_id": "s1", "value": 42}
        assert apply_row_filter(record, None) is True

    def test_empty_dict_criteria_passes_all(self):
        """An empty criteria dict allows all records through."""
        record = {"sensor_id": "s1", "value": 42}
        assert apply_row_filter(record, {}) is True

    def test_empty_record_with_no_criteria(self):
        """An empty record passes when no criteria are specified."""
        assert apply_row_filter({}, None) is True
        assert apply_row_filter({}, {}) is True


class TestApplyRowFilterSingleValue:
    """Tests for exact single-value matching criteria."""

    def test_matching_single_criterion(self):
        """A record matching a single criterion passes."""
        record = {"sensor_id": "sensor-01", "value": 23.5}
        criteria = {"sensor_id": "sensor-01"}
        assert apply_row_filter(record, criteria) is True

    def test_non_matching_single_criterion(self):
        """A record not matching a single criterion fails."""
        record = {"sensor_id": "sensor-02", "value": 23.5}
        criteria = {"sensor_id": "sensor-01"}
        assert apply_row_filter(record, criteria) is False

    def test_matching_multiple_criteria(self):
        """A record matching all multiple criteria passes."""
        record = {"device_id": "device-001", "canton": "ZH", "value": 10}
        criteria = {"device_id": "device-001", "canton": "ZH"}
        assert apply_row_filter(record, criteria) is True

    def test_partial_match_fails(self):
        """A record matching only some criteria fails (AND logic)."""
        record = {"device_id": "device-001", "canton": "BE", "value": 10}
        criteria = {"device_id": "device-001", "canton": "ZH"}
        assert apply_row_filter(record, criteria) is False

    def test_numeric_value_criterion(self):
        """Numeric value criteria work with exact matching."""
        record = {"sensor_id": "s1", "priority": 1}
        criteria = {"priority": 1}
        assert apply_row_filter(record, criteria) is True

    def test_numeric_type_mismatch(self):
        """An integer criterion does not match a string field."""
        record = {"priority": "1"}
        criteria = {"priority": 1}
        assert apply_row_filter(record, criteria) is False


class TestApplyRowFilterListValues:
    """Tests for list-of-values criteria (match any one)."""

    def test_value_in_list_passes(self):
        """A record whose field value is in the acceptable list passes."""
        record = {"canton": "ZH", "value": 10}
        criteria = {"canton": ["ZH", "BE", "LU"]}
        assert apply_row_filter(record, criteria) is True

    def test_value_not_in_list_fails(self):
        """A record whose field value is not in the acceptable list fails."""
        record = {"canton": "TI", "value": 10}
        criteria = {"canton": ["ZH", "BE", "LU"]}
        assert apply_row_filter(record, criteria) is False

    def test_empty_list_criterion_always_fails(self):
        """An empty list criterion means no value is acceptable."""
        record = {"canton": "ZH"}
        criteria = {"canton": []}
        assert apply_row_filter(record, criteria) is False

    def test_mixed_single_and_list_criteria(self):
        """A mix of single-value and list criteria work together."""
        record = {"device_id": "device-001", "canton": "ZH", "status": "active"}
        criteria = {"device_id": "device-001", "canton": ["ZH", "BE"]}
        assert apply_row_filter(record, criteria) is True


class TestApplyRowFilterMissingFields:
    """Tests for records missing required fields."""

    def test_missing_field_fails(self):
        """A record missing a field required by criteria fails."""
        record = {"value": 42}
        criteria = {"sensor_id": "s1"}
        assert apply_row_filter(record, criteria) is False

    def test_missing_one_of_multiple_fields_fails(self):
        """Missing any one criterion field causes failure."""
        record = {"device_id": "device-001"}
        criteria = {"device_id": "device-001", "canton": "ZH"}
        assert apply_row_filter(record, criteria) is False


class TestLoadInclusionCriteria:
    """Tests for loading criteria from environment variable."""

    def test_unset_env_var_returns_empty(self, monkeypatch):
        """An unset ROW_INCLUSION_CRITERIA returns an empty dict."""
        monkeypatch.delenv("ROW_INCLUSION_CRITERIA", raising=False)
        assert load_inclusion_criteria() == {}

    def test_empty_env_var_returns_empty(self, monkeypatch):
        """An empty ROW_INCLUSION_CRITERIA returns an empty dict."""
        monkeypatch.setenv("ROW_INCLUSION_CRITERIA", "")
        assert load_inclusion_criteria() == {}

    def test_whitespace_only_returns_empty(self, monkeypatch):
        """A whitespace-only ROW_INCLUSION_CRITERIA returns an empty dict."""
        monkeypatch.setenv("ROW_INCLUSION_CRITERIA", "   ")
        assert load_inclusion_criteria() == {}

    def test_valid_json_dict_parsed(self, monkeypatch):
        """A valid JSON object is parsed into a dictionary."""
        criteria = {"device_id": "device-001", "canton": ["ZH", "BE", "LU"]}
        monkeypatch.setenv("ROW_INCLUSION_CRITERIA", json.dumps(criteria))
        assert load_inclusion_criteria() == criteria

    def test_invalid_json_raises_value_error(self, monkeypatch):
        """Invalid JSON raises a ValueError."""
        monkeypatch.setenv("ROW_INCLUSION_CRITERIA", "{not valid json")
        with pytest.raises(ValueError, match="invalid JSON"):
            load_inclusion_criteria()

    def test_non_dict_json_raises_value_error(self, monkeypatch):
        """A JSON value that is not an object raises a ValueError."""
        monkeypatch.setenv("ROW_INCLUSION_CRITERIA", '["a", "b"]')
        with pytest.raises(ValueError, match="must be a JSON object"):
            load_inclusion_criteria()
