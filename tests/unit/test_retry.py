"""Unit tests for the retry module.

Tests verify that compute_backoff returns correct values and that
retry_connect handles success, failure, and exit scenarios correctly.
"""

import pytest

from ingestion_service.retry import compute_backoff, retry_connect


class TestComputeBackoff:
    """Tests for the compute_backoff function."""

    def test_attempt_1_returns_1(self):
        """First attempt should have 1 second backoff (2^0 = 1)."""
        assert compute_backoff(1) == 1.0

    def test_attempt_2_returns_2(self):
        """Second attempt should have 2 second backoff (2^1 = 2)."""
        assert compute_backoff(2) == 2.0

    def test_attempt_3_returns_4(self):
        """Third attempt should have 4 second backoff (2^2 = 4)."""
        assert compute_backoff(3) == 4.0

    def test_attempt_4_returns_8(self):
        """Fourth attempt should have 8 second backoff (2^3 = 8)."""
        assert compute_backoff(4) == 8.0

    def test_attempt_5_returns_16(self):
        """Fifth attempt should have 16 second backoff (2^4 = 16)."""
        assert compute_backoff(5) == 16.0

    def test_attempt_6_returns_32(self):
        """Sixth attempt should be capped at 32 seconds (2^5 = 32)."""
        assert compute_backoff(6) == 32.0

    def test_attempt_10_returns_32(self):
        """Tenth attempt should be capped at 32 seconds."""
        assert compute_backoff(10) == 32.0

    def test_attempt_100_returns_32(self):
        """Very large attempt numbers are still capped at 32."""
        assert compute_backoff(100) == 32.0

    def test_returns_float(self):
        """Backoff value should always be a float."""
        assert isinstance(compute_backoff(1), float)
        assert isinstance(compute_backoff(5), float)


class TestRetryConnect:
    """Tests for the retry_connect function."""

    def test_successful_first_attempt(self):
        """Connection that succeeds immediately should return without retrying."""
        call_count = 0

        def connect():
            nonlocal call_count
            call_count += 1
            return "connected"

        result = retry_connect(connect, max_attempts=5, _sleep_fn=lambda _: None)
        assert result == "connected"
        assert call_count == 1

    def test_successful_after_failures(self):
        """Connection that succeeds after failures should stop retrying."""
        call_count = 0

        def connect():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("not ready")
            return "connected"

        result = retry_connect(connect, max_attempts=5, _sleep_fn=lambda _: None)
        assert result == "connected"
        assert call_count == 3

    def test_calls_connect_fn_max_attempts_times_before_exit(self):
        """After max_attempts failures, should have called connect_fn exactly max_attempts times."""
        call_count = 0

        def connect():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("unavailable")

        with pytest.raises(SystemExit) as exc_info:
            retry_connect(connect, max_attempts=5, _sleep_fn=lambda _: None)

        assert call_count == 5
        assert exc_info.value.code == 1

    def test_exits_with_non_zero_code_after_max_failures(self):
        """After max consecutive failures, process should exit with code 1."""

        def connect():
            raise RuntimeError("always fails")

        with pytest.raises(SystemExit) as exc_info:
            retry_connect(connect, max_attempts=10, _sleep_fn=lambda _: None)

        assert exc_info.value.code == 1

    def test_default_max_attempts_is_10(self):
        """Default max_attempts should be 10."""
        call_count = 0

        def connect():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("fail")

        with pytest.raises(SystemExit):
            retry_connect(connect, _sleep_fn=lambda _: None)

        assert call_count == 10

    def test_sleep_called_with_correct_backoff(self):
        """Sleep should be called with computed backoff values between attempts."""
        sleep_values = []

        def mock_sleep(seconds):
            sleep_values.append(seconds)

        def connect():
            raise ConnectionError("fail")

        with pytest.raises(SystemExit):
            retry_connect(connect, max_attempts=4, _sleep_fn=mock_sleep)

        # Sleep is called between attempts (not after the last failure)
        # Attempt 1 fails -> sleep(1), Attempt 2 fails -> sleep(2), Attempt 3 fails -> sleep(4)
        assert sleep_values == [1.0, 2.0, 4.0]

    def test_base_interval_multiplier(self):
        """base_interval should multiply the backoff delay."""
        sleep_values = []

        def mock_sleep(seconds):
            sleep_values.append(seconds)

        def connect():
            raise ConnectionError("fail")

        with pytest.raises(SystemExit):
            retry_connect(
                connect, max_attempts=3, base_interval=2.0, _sleep_fn=mock_sleep
            )

        # With base_interval=2.0: attempt 1 -> 1*2=2, attempt 2 -> 2*2=4
        assert sleep_values == [2.0, 4.0]

    def test_returns_value_from_connect_fn(self):
        """Should return whatever value connect_fn returns on success."""

        def connect():
            return {"status": "ok", "broker": "kafka:9092"}

        result = retry_connect(connect, _sleep_fn=lambda _: None)
        assert result == {"status": "ok", "broker": "kafka:9092"}
