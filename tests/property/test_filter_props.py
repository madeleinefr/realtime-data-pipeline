"""Property-based tests for row-level filtering.

Uses Hypothesis to verify that apply_row_filter correctly implements AND-based
inclusion filtering across a wide range of generated records and criteria.
"""

import pytest
from hypothesis import given, settings, assume
from hypothesis import strategies as st

from processing_service.filter import apply_row_filter


# --- Strategies ---

# Strategy for field names (non-empty strings)
field_names = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N", "Pd")),
    min_size=1,
    max_size=20,
)

# Strategy for simple comparable values (strings, ints, floats, bools)
simple_values = st.one_of(
    st.text(min_size=0, max_size=50),
    st.integers(min_value=-1_000_000, max_value=1_000_000),
    st.floats(
        min_value=-1e6,
        max_value=1e6,
        allow_nan=False,
        allow_infinity=False,
    ),
    st.booleans(),
)

# Strategy for generating records
records = st.dictionaries(
    keys=field_names,
    values=simple_values,
    min_size=0,
    max_size=10,
)

# Strategy for single-value criteria (matching a record field exactly)
single_value_criteria = simple_values

# Strategy for list-value criteria
list_value_criteria = st.lists(simple_values, min_size=1, max_size=5)


class TestEmptyCriteriaProperties:
    """Property tests for empty/None criteria behavior.

    **Validates: NFR-5.9, NFR-6.6**
    """

    @settings(max_examples=200)
    @given(record=records)
    def test_none_criteria_always_passes(self, record):
        """**Validates: NFR-5.9**

        Any record passes when inclusion_criteria is None.
        """
        assert apply_row_filter(record, None) is True

    @settings(max_examples=200)
    @given(record=records)
    def test_empty_criteria_always_passes(self, record):
        """**Validates: NFR-5.9**

        Any record passes when inclusion_criteria is an empty dict.
        """
        assert apply_row_filter(record, {}) is True


class TestExactMatchProperties:
    """Property tests for exact-value criterion matching.

    **Validates: NFR-5.9, NFR-6.6, NFR-6.7**
    """

    @settings(max_examples=200)
    @given(record=records)
    def test_criteria_from_record_always_passes(self, record):
        """**Validates: NFR-5.9**

        If criteria are constructed from the record's own field values,
        the record always passes.
        """
        assume(len(record) > 0)
        # Use the record's own values as criteria — must always pass
        criteria = dict(record)
        assert apply_row_filter(record, criteria) is True

    @settings(max_examples=200)
    @given(record=records, extra_field=field_names, extra_value=simple_values)
    def test_missing_field_always_fails(self, record, extra_field, extra_value):
        """**Validates: NFR-6.6**

        If criteria reference a field not present in the record, the record
        always fails.
        """
        assume(extra_field not in record)
        criteria = {extra_field: extra_value}
        assert apply_row_filter(record, criteria) is False

    @settings(max_examples=200)
    @given(
        field_name=field_names,
        record_value=simple_values,
        criterion_value=simple_values,
    )
    def test_mismatched_value_fails(self, field_name, record_value, criterion_value):
        """**Validates: NFR-6.6**

        If a criterion value differs from the record's field value, the
        record fails.
        """
        assume(record_value != criterion_value)
        record = {field_name: record_value}
        criteria = {field_name: criterion_value}
        assert apply_row_filter(record, criteria) is False


class TestListCriteriaProperties:
    """Property tests for list-of-values criteria.

    **Validates: NFR-5.9, NFR-6.6**
    """

    @settings(max_examples=200)
    @given(field_name=field_names, value=simple_values)
    def test_value_in_list_passes(self, field_name, value):
        """**Validates: NFR-5.9**

        If the record's field value appears in the criterion list, the
        record passes.
        """
        record = {field_name: value}
        criteria = {field_name: [value]}
        assert apply_row_filter(record, criteria) is True

    @settings(max_examples=200)
    @given(
        field_name=field_names,
        record_value=simple_values,
        criterion_values=st.lists(simple_values, min_size=1, max_size=5),
    )
    def test_value_not_in_list_fails(self, field_name, record_value, criterion_values):
        """**Validates: NFR-6.6**

        If the record's field value is not in the criterion list, the record
        fails.
        """
        assume(record_value not in criterion_values)
        record = {field_name: record_value}
        criteria = {field_name: criterion_values}
        assert apply_row_filter(record, criteria) is False


class TestReturnTypeProperty:
    """Property tests for return type guarantees.

    **Validates: NFR-6.7**
    """

    @settings(max_examples=200)
    @given(record=records)
    def test_always_returns_bool(self, record):
        """**Validates: NFR-6.7**

        apply_row_filter always returns a boolean value regardless of input.
        """
        # Test with None criteria
        result_none = apply_row_filter(record, None)
        assert isinstance(result_none, bool)

        # Test with empty criteria
        result_empty = apply_row_filter(record, {})
        assert isinstance(result_empty, bool)

        # Test with criteria from the record itself
        if record:
            result_self = apply_row_filter(record, dict(record))
            assert isinstance(result_self, bool)

    @settings(max_examples=200)
    @given(record=records, field=field_names, value=simple_values)
    def test_never_raises_for_valid_inputs(self, record, field, value):
        """**Validates: NFR-6.7**

        apply_row_filter never raises an exception for valid record/criteria
        combinations.
        """
        criteria = {field: value}
        # Should not raise — just return True or False
        result = apply_row_filter(record, criteria)
        assert isinstance(result, bool)
