"""Property-based tests for data transformation.

Uses Hypothesis to verify that apply_transformations correctly applies
field exclusion, whitespace trimming, and numeric normalization across
a wide range of generated inputs.
"""

import pytest
from hypothesis import given, settings, assume
from hypothesis import strategies as st

from processing_service.transform import apply_transformations


# --- Strategies ---

# Strategy for field names (non-empty strings without leading/trailing whitespace)
field_names = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N", "Pd")),
    min_size=1,
    max_size=20,
)

# Strategy for string values (may include whitespace)
string_values = st.text(min_size=0, max_size=100)

# Strategy for numeric values
numeric_values = st.one_of(
    st.integers(min_value=-1_000_000, max_value=1_000_000),
    st.floats(
        min_value=-1e6,
        max_value=1e6,
        allow_nan=False,
        allow_infinity=False,
    ),
)

# Strategy for mixed record values
record_values = st.one_of(
    string_values,
    numeric_values,
    st.booleans(),
    st.none(),
    st.lists(st.integers(), max_size=3),
)

# Strategy for generating records
records = st.dictionaries(
    keys=field_names,
    values=record_values,
    min_size=0,
    max_size=10,
)

# Strategy for normalization scaling factors (non-zero finite floats)
scaling_factors = st.floats(
    min_value=-1e6,
    max_value=1e6,
    allow_nan=False,
    allow_infinity=False,
).filter(lambda x: x != 0.0)


class TestExclusionProperties:
    """Property tests for column-level filtering (field exclusion).

    **Validates: FR-3.3, NFR-6.7**
    """

    @settings(max_examples=200)
    @given(record=records, extra_keys=st.lists(field_names, max_size=5))
    def test_excluded_fields_never_appear_in_output(self, record, extra_keys):
        """**Validates: FR-3.3**

        For any record and any exclusion list, no excluded field appears in
        the output.
        """
        exclusion_list = list(record.keys())[:3] + extra_keys
        result = apply_transformations(record, exclusion_list)
        for key in exclusion_list:
            assert key not in result

    @settings(max_examples=200)
    @given(record=records)
    def test_non_excluded_fields_always_present(self, record):
        """**Validates: FR-3.3**

        For any record with an empty exclusion list, all original keys are
        preserved in the output.
        """
        result = apply_transformations(record, [])
        assert set(result.keys()) == set(record.keys())

    @settings(max_examples=200)
    @given(record=records, exclusion_list=st.lists(field_names, max_size=5))
    def test_output_keys_are_subset_of_input_keys(self, record, exclusion_list):
        """**Validates: FR-3.3**

        The output keys are always a subset of the input keys (no new keys
        are ever introduced).
        """
        result = apply_transformations(record, exclusion_list)
        assert set(result.keys()).issubset(set(record.keys()))


class TestTrimmingProperties:
    """Property tests for whitespace trimming on string fields.

    **Validates: FR-3.3, NFR-6.7**
    """

    @settings(max_examples=200)
    @given(record=st.dictionaries(field_names, string_values, min_size=1, max_size=10))
    def test_string_values_have_no_leading_trailing_whitespace(self, record):
        """**Validates: FR-3.3**

        After transformation, no string value has leading or trailing whitespace.
        """
        result = apply_transformations(record, [])
        for value in result.values():
            if isinstance(value, str):
                assert value == value.strip()

    @settings(max_examples=200)
    @given(
        record=st.dictionaries(field_names, string_values, min_size=1, max_size=10)
    )
    def test_trimmed_values_equal_strip_of_original(self, record):
        """**Validates: FR-3.3**

        Each string value in the output equals the stripped version of the
        corresponding input value.
        """
        result = apply_transformations(record, [])
        for key in record:
            if isinstance(record[key], str):
                assert result[key] == record[key].strip()


class TestNormalizationProperties:
    """Property tests for numeric field normalization.

    **Validates: FR-3.3, NFR-6.7**
    """

    @settings(max_examples=200)
    @given(
        field_name=field_names,
        value=numeric_values,
        factor=scaling_factors,
    )
    def test_normalized_value_equals_original_times_factor(
        self, field_name, value, factor
    ):
        """**Validates: FR-3.3**

        A numeric field in the normalization config is multiplied by its
        scaling factor.
        """
        record = {field_name: value}
        config = {field_name: factor}
        result = apply_transformations(record, [], normalization_config=config)
        expected = value * factor
        assert result[field_name] == pytest.approx(expected, rel=1e-9, abs=1e-12)

    @settings(max_examples=200)
    @given(
        field_name=field_names,
        value=numeric_values,
    )
    def test_numeric_field_not_in_config_unchanged(self, field_name, value):
        """**Validates: FR-3.3**

        A numeric field not listed in the normalization config retains its
        original value.
        """
        record = {field_name: value}
        config = {}  # empty config — no normalization
        result = apply_transformations(record, [], normalization_config=config)
        assert result[field_name] == value


class TestImmutabilityProperty:
    """Property tests verifying the input record is never mutated.

    **Validates: NFR-6.7**
    """

    @settings(max_examples=200)
    @given(
        record=records,
        exclusion_list=st.lists(field_names, max_size=5),
    )
    def test_original_record_is_never_mutated(self, record, exclusion_list):
        """**Validates: NFR-6.7**

        The original record dictionary is not modified by apply_transformations.
        """
        original_copy = dict(record)
        apply_transformations(record, exclusion_list)
        assert record == original_copy
