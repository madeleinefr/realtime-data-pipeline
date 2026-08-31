"""Unit tests for the Ingestion_Service.

Example-based unit tests covering the ingestion service's core behaviors:
    - Deterministic record generation with specific ``random`` seeds
      (Requirements FR-1.2, FR-1.4).
    - Configurable produce-rate / rate limiting behavior
      (Requirement FR-1.3).
    - Retry logic with exact exponential backoff values for attempts 1-10
      and non-zero exit after 10 consecutive failures
      (Requirements FR-1.6, FR-1.7, NFR-3.4).
    - Connection loss handling that exits non-zero after losing the broker
      connection for more than 60 seconds (Requirement NFR-3.4).

These complement the property-based tests in ``tests/property/`` by pinning
down specific seeds, exact values, and edge cases.
"""

import random
from datetime import datetime, timezone

import pytest

import ingestion_service.generator as generator
import ingestion_service.main as main
from ingestion_service.generator import generate_device_record
from ingestion_service.main import (
    CONNECTION_LOSS_TIMEOUT,
    get_produce_rate,
    get_record_limit,
)
from ingestion_service.retry import compute_backoff, retry_connect


# Fields whose values are driven purely by the ``random`` module and are
# therefore deterministic once ``random.seed`` is fixed. ``record_id`` (uuid4)
# and ``timestamp`` (wall-clock) are intentionally excluded.
_DETERMINISTIC_FIELDS = [
    "device_id",
    "device_type",
    "canton",
    "latitude",
    "longitude",
    "temperature",
    "humidity",
    "pressure",
    "status",
]


@pytest.fixture(autouse=True)
def _reset_generator_state():
    """Reset the generator's monotonic timestamp state between tests."""
    generator._last_timestamp = datetime.now(timezone.utc)
    yield
    generator._last_timestamp = datetime.now(timezone.utc)


def _deterministic_subset(record):
    """Return only the seed-deterministic fields of a generated record."""
    return {field: record[field] for field in _DETERMINISTIC_FIELDS}


class TestRecordGenerationWithSeeds:
    """Record generation with specific seeds.

    Validates: Requirements FR-1.2, FR-1.4
    """

    def test_same_seed_produces_identical_records(self):
        """Seeding ``random`` with the same value yields identical field values."""
        random.seed(42)
        first = _deterministic_subset(generate_device_record())

        random.seed(42)
        second = _deterministic_subset(generate_device_record())

        assert first == second

    def test_different_seeds_produce_different_records(self):
        """Different seeds should yield different records (sanity check)."""
        random.seed(1)
        first = _deterministic_subset(generate_device_record())

        random.seed(2)
        second = _deterministic_subset(generate_device_record())

        assert first != second

    def test_seed_produces_deterministic_sequence(self):
        """A fixed seed reproduces an entire sequence of records deterministically."""
        random.seed(1234)
        sequence_a = [
            _deterministic_subset(generate_device_record()) for _ in range(5)
        ]

        random.seed(1234)
        sequence_b = [
            _deterministic_subset(generate_device_record()) for _ in range(5)
        ]

        assert sequence_a == sequence_b

    def test_seeded_record_has_valid_field_values(self):
        """A seeded record must still satisfy the documented field constraints."""
        random.seed(7)
        record = generate_device_record()

        assert record["device_id"].startswith("device-")
        device_number = int(record["device_id"].split("-")[1])
        assert 1 <= device_number <= 500

        assert record["device_type"] == "weather_station"
        assert record["canton"] in generator.SWISS_CANTONS
        assert record["status"] in generator.DEVICE_STATUSES

        assert generator.LATITUDE_MIN <= record["latitude"] <= generator.LATITUDE_MAX
        assert (
            generator.LONGITUDE_MIN <= record["longitude"] <= generator.LONGITUDE_MAX
        )
        assert (
            generator.TEMPERATURE_MIN
            <= record["temperature"]
            <= generator.TEMPERATURE_MAX
        )
        assert generator.HUMIDITY_MIN <= record["humidity"] <= generator.HUMIDITY_MAX
        assert generator.PRESSURE_MIN <= record["pressure"] <= generator.PRESSURE_MAX

    def test_timestamps_non_decreasing_for_seeded_sequence(self):
        """Timestamps remain monotonically non-decreasing regardless of seed."""
        random.seed(99)
        records = [generate_device_record() for _ in range(10)]
        timestamps = [datetime.fromisoformat(r["timestamp"]) for r in records]
        for i in range(1, len(timestamps)):
            assert timestamps[i] >= timestamps[i - 1]

    def test_record_id_is_non_deterministic_uuid(self):
        """record_id uses uuid4 and is unique even under a fixed seed."""
        random.seed(500)
        first = generate_device_record()["record_id"]
        random.seed(500)
        second = generate_device_record()["record_id"]
        assert first != second


class TestRateLimiting:
    """Rate limiting / configurable produce-rate behavior.

    Validates: Requirement FR-1.3
    """

    def test_default_rate_is_one_per_second(self, monkeypatch):
        """Unset PRODUCE_RATE defaults to 1.0 records/second."""
        monkeypatch.delenv("PRODUCE_RATE", raising=False)
        assert get_produce_rate() == 1.0

    def test_configured_rate_is_read_from_env(self, monkeypatch):
        """A valid PRODUCE_RATE env value is used directly."""
        monkeypatch.setenv("PRODUCE_RATE", "100")
        assert get_produce_rate() == 100.0

    def test_high_rate_is_respected(self, monkeypatch):
        """The upper end of the configurable range (10,000) is honored."""
        monkeypatch.setenv("PRODUCE_RATE", "10000")
        assert get_produce_rate() == 10000.0

    def test_rate_maps_to_expected_interval(self, monkeypatch):
        """The produce interval is the reciprocal of the configured rate."""
        monkeypatch.setenv("PRODUCE_RATE", "100")
        rate = get_produce_rate()
        interval = 1.0 / rate
        assert interval == pytest.approx(0.01)

        monkeypatch.setenv("PRODUCE_RATE", "10000")
        rate = get_produce_rate()
        interval = 1.0 / rate
        assert interval == pytest.approx(0.0001)

    def test_zero_rate_falls_back_to_default(self, monkeypatch):
        """A non-positive rate is rejected in favor of the 1.0 default."""
        monkeypatch.setenv("PRODUCE_RATE", "0")
        assert get_produce_rate() == 1.0

    def test_negative_rate_falls_back_to_default(self, monkeypatch):
        """A negative rate is rejected in favor of the 1.0 default."""
        monkeypatch.setenv("PRODUCE_RATE", "-5")
        assert get_produce_rate() == 1.0

    def test_invalid_rate_falls_back_to_default(self, monkeypatch):
        """A non-numeric rate is rejected in favor of the 1.0 default."""
        monkeypatch.setenv("PRODUCE_RATE", "not-a-number")
        assert get_produce_rate() == 1.0

    def test_run_loop_sleeps_for_configured_interval(self, monkeypatch):
        """The run loop sleeps for 1/rate seconds between records (rate limiting)."""
        monkeypatch.setenv("PRODUCE_RATE", "100")
        monkeypatch.setenv("RECORD_LIMIT", "3")
        monkeypatch.setenv("KAFKA_TOPIC", "iot-sensor-data")

        sleep_calls = []
        produced = []

        class FakeProducer:
            def flush(self, timeout=None):
                pass

        fake_producer = FakeProducer()

        monkeypatch.setattr(main, "retry_connect", lambda _fn: fake_producer)
        monkeypatch.setattr(main, "generate_device_record", lambda: {"device_id": "device-001"})
        monkeypatch.setattr(
            main, "produce_record", lambda p, t, r: produced.append((t, r))
        )
        monkeypatch.setattr(main.time, "sleep", lambda s: sleep_calls.append(s))

        main.run()

        # 3 records produced, and each iteration sleeps for the 1/100 interval.
        assert len(produced) == 3
        assert sleep_calls == [pytest.approx(0.01)] * 3


class TestRecordLimit:
    """Record-limit configuration parsing (supports controlled run-loop tests)."""

    def test_unset_limit_is_none(self, monkeypatch):
        monkeypatch.delenv("RECORD_LIMIT", raising=False)
        assert get_record_limit() is None

    def test_empty_limit_is_none(self, monkeypatch):
        monkeypatch.setenv("RECORD_LIMIT", "")
        assert get_record_limit() is None

    def test_valid_limit_is_parsed(self, monkeypatch):
        monkeypatch.setenv("RECORD_LIMIT", "1000000")
        assert get_record_limit() == 1_000_000

    def test_non_positive_limit_is_unlimited(self, monkeypatch):
        monkeypatch.setenv("RECORD_LIMIT", "0")
        assert get_record_limit() is None

    def test_invalid_limit_is_unlimited(self, monkeypatch):
        monkeypatch.setenv("RECORD_LIMIT", "abc")
        assert get_record_limit() is None


class TestRetryBackoffExactValues:
    """Retry logic with exact backoff values for attempts 1-10.

    Validates: Requirements FR-1.6, FR-1.7, NFR-3.4
    """

    # Expected backoff per attempt: min(2^(attempt-1), 32).
    EXPECTED_BACKOFF = {
        1: 1.0,
        2: 2.0,
        3: 4.0,
        4: 8.0,
        5: 16.0,
        6: 32.0,
        7: 32.0,
        8: 32.0,
        9: 32.0,
        10: 32.0,
    }

    @pytest.mark.parametrize("attempt,expected", sorted(EXPECTED_BACKOFF.items()))
    def test_compute_backoff_exact_value(self, attempt, expected):
        """compute_backoff returns the exact expected value for attempts 1-10."""
        assert compute_backoff(attempt) == expected

    def test_backoff_sequence_for_attempts_1_to_10(self):
        """The full backoff sequence for attempts 1-10 matches the spec exactly."""
        actual = [compute_backoff(attempt) for attempt in range(1, 11)]
        expected = [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 32.0, 32.0, 32.0, 32.0]
        assert actual == expected

    def test_sleep_backoff_sequence_across_10_failures(self):
        """retry_connect sleeps with the exact backoff between attempts 1-9.

        With max_attempts=10, sleeps occur after attempts 1 through 9 (no sleep
        after the final failure), producing the sequence 1,2,4,8,16,32,32,32,32.
        """
        sleep_values = []

        def always_fail():
            raise ConnectionError("broker unavailable")

        with pytest.raises(SystemExit) as exc_info:
            retry_connect(
                always_fail,
                max_attempts=10,
                _sleep_fn=lambda s: sleep_values.append(s),
            )

        assert exc_info.value.code == 1
        assert sleep_values == [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 32.0, 32.0, 32.0]

    def test_exits_non_zero_after_10_consecutive_failures(self):
        """After 10 consecutive failures the service exits with a non-zero code."""
        attempts = 0

        def always_fail():
            nonlocal attempts
            attempts += 1
            raise ConnectionError("unavailable")

        with pytest.raises(SystemExit) as exc_info:
            retry_connect(always_fail, max_attempts=10, _sleep_fn=lambda _: None)

        assert attempts == 10
        assert exc_info.value.code != 0

    def test_succeeds_within_10_attempts_does_not_exit(self):
        """A connection that recovers before attempt 10 returns normally."""
        attempts = 0

        def flaky():
            nonlocal attempts
            attempts += 1
            if attempts < 10:
                raise ConnectionError("still warming up")
            return "connected"

        result = retry_connect(flaky, max_attempts=10, _sleep_fn=lambda _: None)
        assert result == "connected"
        assert attempts == 10


class TestConnectionLossHandling:
    """Connection loss handling (>60s timeout).

    Validates: Requirement NFR-3.4
    """

    def test_connection_loss_timeout_is_60_seconds(self):
        """The configured connection-loss timeout is exactly 60 seconds."""
        assert CONNECTION_LOSS_TIMEOUT == 60.0

    def _run_with_broker_down(self, monkeypatch, elapsed_seconds):
        """Drive main.run() with produce_record failing and a controlled clock.

        Returns a dict recording whether flush() was called and the exit code.
        """
        monkeypatch.setenv("KAFKA_TOPIC", "iot-sensor-data")
        monkeypatch.setenv("PRODUCE_RATE", "1.0")
        monkeypatch.delenv("RECORD_LIMIT", raising=False)

        state = {"flushed": False}

        class FakeProducer:
            def flush(self, timeout=None):
                state["flushed"] = True

        monkeypatch.setattr(main, "retry_connect", lambda _fn: FakeProducer())
        monkeypatch.setattr(
            main, "generate_device_record", lambda: {"device_id": "device-001"}
        )

        # Controlled clock keyed on the produce-failure event rather than on
        # call count. It stays at the baseline (0.0) while run() sets
        # last_successful_produce, then advances to elapsed_seconds when a
        # produce failure occurs so the elapsed-time check inside the except
        # block observes it. Tolerates unlimited extra calls (e.g. from the
        # structured logger's use of time.time() via record.created).
        clock = {"t": 0.0}
        monkeypatch.setattr(main.time, "time", lambda: clock["t"])

        def failing_produce(_producer, _topic, _record):
            clock["t"] = elapsed_seconds
            raise ConnectionError("broker connection lost")

        monkeypatch.setattr(main, "produce_record", failing_produce)
        monkeypatch.setattr(main.time, "sleep", lambda _s: None)

        return state

    def test_exits_non_zero_when_connection_lost_over_60s(self, monkeypatch):
        """Losing the broker for >60s flushes and exits with a non-zero code."""
        state = self._run_with_broker_down(monkeypatch, elapsed_seconds=61.0)

        with pytest.raises(SystemExit) as exc_info:
            main.run()

        assert exc_info.value.code != 0
        assert state["flushed"] is True

    def test_does_not_exit_when_loss_within_60s(self, monkeypatch):
        """A brief failure (<=60s) does not trigger the shutdown/exit path."""
        monkeypatch.setenv("KAFKA_TOPIC", "iot-sensor-data")
        monkeypatch.setenv("PRODUCE_RATE", "1.0")
        monkeypatch.delenv("RECORD_LIMIT", raising=False)

        state = {"flushed": False}

        class FakeProducer:
            def flush(self, timeout=None):
                state["flushed"] = True

        # Sentinel used to break out of the otherwise-infinite loop after we
        # have observed the sub-threshold failure was handled without exiting.
        class _StopLoop(Exception):
            pass

        produce_calls = {"count": 0}

        # Controlled clock keyed on the produce-failure event rather than on
        # call count: baseline 0.0 while run() sets last_successful_produce,
        # then 30.0s (<= 60 threshold, no exit) once a produce failure occurs.
        # Tolerates unlimited extra calls (e.g. from the structured logger).
        clock = {"t": 0.0}

        def failing_produce(_producer, _topic, _record):
            produce_calls["count"] += 1
            clock["t"] = 30.0
            raise ConnectionError("transient blip")

        monkeypatch.setattr(main, "retry_connect", lambda _fn: FakeProducer())
        monkeypatch.setattr(
            main, "generate_device_record", lambda: {"device_id": "device-001"}
        )
        monkeypatch.setattr(main, "produce_record", failing_produce)
        monkeypatch.setattr(main.time, "time", lambda: clock["t"])

        # After the (successfully handled) failure, sleep() is reached, which we
        # use as a deterministic breakpoint proving run() did not exit early.
        def stop_on_sleep(_s):
            raise _StopLoop()

        monkeypatch.setattr(main.time, "sleep", stop_on_sleep)

        with pytest.raises(_StopLoop):
            main.run()

        # The failure was handled and the loop proceeded to sleep(); no >60s
        # shutdown flush or SystemExit occurred.
        assert produce_calls["count"] == 1
        assert state["flushed"] is False
