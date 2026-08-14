"""Retry logic with exponential backoff for the ingestion service.

Provides connection retry functionality with exponential backoff to handle
transient failures when connecting to external services (e.g., Kafka brokers).
After a configurable number of consecutive failures, the service exits with
a non-zero code to signal unrecoverable failure.

Requirements: FR-1.6, FR-1.7, NFR-3.4
"""

import sys
import time
from typing import Callable, TypeVar

from ingestion_service.logger import get_logger

T = TypeVar("T")

logger = get_logger("retry")


def compute_backoff(attempt: int) -> float:
    """Compute the backoff delay for a given retry attempt.

    Uses exponential backoff with a cap at 32 seconds.
    Formula: min(2^(attempt-1), 32)

    Args:
        attempt: The retry attempt number (1-based, must be >= 1).

    Returns:
        The backoff delay in seconds (between 1.0 and 32.0).
    """
    return float(min(2 ** (attempt - 1), 32))


def retry_connect(
    connect_fn: Callable[[], T],
    max_attempts: int = 10,
    base_interval: float = 1.0,
    _sleep_fn: Callable[[float], None] = time.sleep,
) -> T:
    """Retry a connection function with exponential backoff.

    Calls connect_fn repeatedly until it succeeds or max_attempts consecutive
    failures are reached. Each retry attempt is logged with a timestamp via
    structured JSON logging. After max_attempts consecutive failures, the
    process exits with a non-zero code.

    Args:
        connect_fn: A callable that attempts a connection. Should raise an
            exception on failure and return a value on success.
        max_attempts: Maximum number of consecutive failures before exiting.
            Defaults to 10.
        base_interval: Base interval multiplier for backoff calculation.
            Defaults to 1.0.
        _sleep_fn: Sleep function for testing purposes. Defaults to time.sleep.

    Returns:
        The return value of connect_fn on success.

    Raises:
        SystemExit: If max_attempts consecutive failures occur, exits with
            code 1.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            result = connect_fn()
            if attempt > 1:
                logger.info(
                    f"Connection succeeded on attempt {attempt}"
                )
            return result
        except Exception as e:
            backoff = compute_backoff(attempt) * base_interval
            logger.warning(
                f"Connection attempt {attempt}/{max_attempts} failed: {e}. "
                f"Retrying in {backoff:.1f}s"
            )
            if attempt < max_attempts:
                _sleep_fn(backoff)

    logger.error(
        f"All {max_attempts} connection attempts failed. Exiting."
    )
    sys.exit(1)
