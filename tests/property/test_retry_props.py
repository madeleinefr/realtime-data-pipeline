"""Property-based tests for retry logic with exponential backoff.

Uses Hypothesis to verify that compute_backoff satisfies mathematical
properties across all valid inputs.

**Validates: Requirements FR-1.6, FR-1.7, NFR-3.4**
"""

from hypothesis import given, settings
from hypothesis import strategies as st

from ingestion_service.retry import compute_backoff

# Strategy for positive attempt numbers (1-based)
attempt_numbers = st.integers(min_value=1, max_value=1000)


class TestBackoffBoundedProperty:
    """Property: For any positive attempt number, backoff is between 1 and 32 inclusive.

    **Validates: Requirements FR-1.6, FR-1.7**
    """

    @settings(max_examples=200)
    @given(attempt=attempt_numbers)
    def test_backoff_always_between_1_and_32(self, attempt):
        """**Validates: Requirements FR-1.6, FR-1.7**

        For any positive attempt number, compute_backoff must return
        a value in the range [1, 32].
        """
        result = compute_backoff(attempt)
        assert 1.0 <= result <= 32.0, (
            f"Backoff for attempt {attempt} was {result}, expected between 1 and 32"
        )


class TestBackoffMonotonicProperty:
    """Property: Backoff is monotonically non-decreasing.

    **Validates: Requirements FR-1.6, NFR-3.4**
    """

    @settings(max_examples=200)
    @given(attempt=st.integers(min_value=1, max_value=999))
    def test_backoff_monotonically_non_decreasing(self, attempt):
        """**Validates: Requirements FR-1.6, NFR-3.4**

        For any two consecutive attempts, the backoff for the later attempt
        must be greater than or equal to the backoff for the earlier attempt.
        """
        current = compute_backoff(attempt)
        next_val = compute_backoff(attempt + 1)
        assert next_val >= current, (
            f"Backoff decreased from attempt {attempt} ({current}) "
            f"to attempt {attempt + 1} ({next_val})"
        )


class TestBackoffPowerOfTwoProperty:
    """Property: Backoff is always a power of 2 (up to the cap of 32).

    **Validates: Requirements FR-1.6, FR-1.7**
    """

    @settings(max_examples=200)
    @given(attempt=attempt_numbers)
    def test_backoff_is_power_of_two(self, attempt):
        """**Validates: Requirements FR-1.6, FR-1.7**

        For any positive attempt number, compute_backoff must return
        a value that is a power of 2 (1, 2, 4, 8, 16, or 32).
        """
        result = compute_backoff(attempt)
        valid_powers = {1.0, 2.0, 4.0, 8.0, 16.0, 32.0}
        assert result in valid_powers, (
            f"Backoff for attempt {attempt} was {result}, "
            f"expected one of {sorted(valid_powers)}"
        )


class TestBackoffExponentialCorrectnessProperty:
    """Property: For any retry attempt N (1 ≤ N ≤ 10), backoff equals min(2^(N-1), 32).

    Verifies the exact exponential backoff formula is correctly implemented
    for all valid retry attempts within the configured maximum.

    **Validates: Requirements FR-1.6**
    """

    @settings(max_examples=200)
    @given(attempt=st.integers(min_value=1, max_value=10))
    def test_backoff_equals_exponential_formula(self, attempt):
        """**Validates: Requirements FR-1.6**

        For any retry attempt N in [1, 10], compute_backoff(N) must equal
        min(2^(N-1), 32), confirming the exponential backoff formula is
        correctly implemented.
        """
        result = compute_backoff(attempt)
        expected = float(min(2 ** (attempt - 1), 32))
        assert result == expected, (
            f"Backoff for attempt {attempt} was {result}, "
            f"expected {expected} (min(2^{attempt - 1}, 32))"
        )
