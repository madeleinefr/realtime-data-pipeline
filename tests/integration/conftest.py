"""Shared fixtures and skip-guards for integration tests.

Integration tests in this package exercise the real-time data pipeline against
a *running* Docker Compose stack (two Kafka brokers with SASL/PLAIN auth and
ACLs enabled). Because that infrastructure is not available in every
environment (CI, unit-test-only runs, machines without ``confluent-kafka``),
every test in this package is guarded so that it is *skipped* rather than
*failed* when the prerequisites are absent.

Skip conditions (any one triggers a skip):
    1. The ``confluent-kafka`` client library is not importable.
    2. The ``KAFKA_BOOTSTRAP_SERVERS`` environment variable is not set (i.e.
       the caller has not opted in to running against live infrastructure).
    3. The configured Kafka brokers are not reachable within a short timeout.

The module is import-safe: it never requires a running broker just to collect
tests. All broker interaction happens lazily inside fixtures/functions, never
at import time.

Validated requirements (across the integration suite):
    FR-2.3, FR-2.4, FR-2.6, FR-3.5, NFR-3.3, NFR-5.3, NFR-5.6, NFR-5.7.
"""

import os
from typing import Any, Dict, Optional

import pytest

# --- Topic, consumer-group and credential constants (aligned with the spec) ---

RAW_DATA_TOPIC = "raw_data"
AGGREGATED_DATA_TOPIC = "aggregated_data"

PROCESSING_GROUP = "processing-group"
VISUALIZATION_GROUP = "visualization-group"

# Prefix reserved for ephemeral integration-test consumer groups. The cluster
# grants the ``processing`` user READ on any group whose id begins with this
# prefix (see the 'test-' prefixed group ACL in ``kafka/create-topics.sh``),
# which lets FR-2.3 spin up two independent, authorized groups on the same
# topic without weakening the exact-group least-privilege ACLs used in
# production.
TEST_GROUP_PREFIX = "test-"

# Default bootstrap servers used only for display/help; tests require the env
# var to be explicitly set to opt in to live-infrastructure runs.
DEFAULT_BOOTSTRAP = "kafka1:9092,kafka2:9092"

# Per-service SASL credentials. Values fall back to the documented defaults in
# ``.env.example`` but may be overridden via environment variables so the tests
# work against a stack started with custom secrets.
CREDENTIALS = {
    "admin": (
        os.environ.get("KAFKA_ADMIN_USER", "admin"),
        os.environ.get("KAFKA_ADMIN_PASS", "admin-secret"),
    ),
    "ingestion": (
        os.environ.get("INGESTION_KAFKA_USER", "ingestion"),
        os.environ.get("INGESTION_KAFKA_PASS", "ingestion-secret"),
    ),
    "processing": (
        os.environ.get("PROCESSING_KAFKA_USER", "processing"),
        os.environ.get("PROCESSING_KAFKA_PASS", "processing-secret"),
    ),
    "visualization": (
        os.environ.get("VISUALIZATION_KAFKA_USER", "visualization"),
        os.environ.get("VISUALIZATION_KAFKA_PASS", "visualization-secret"),
    ),
}

# --- Optional dependency detection (import-safe) ------------------------------

try:  # pragma: no cover - exercised only when the library is installed
    import confluent_kafka  # noqa: F401

    CONFLUENT_KAFKA_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised only when missing
    CONFLUENT_KAFKA_AVAILABLE = False


def get_bootstrap_servers() -> Optional[str]:
    """Return the configured Kafka bootstrap servers, or None if unset.

    Reading the ``KAFKA_BOOTSTRAP_SERVERS`` environment variable is the opt-in
    signal that a live cluster is available. When it is unset, integration
    tests skip.

    Returns:
        The bootstrap server string (e.g. ``"kafka1:9092,kafka2:9092"``), or
        ``None`` when the environment variable is not set.
    """
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS")


def sasl_config(role: str) -> Dict[str, Any]:
    """Build a confluent-kafka client config dict for the given service role.

    Args:
        role: One of ``"admin"``, ``"ingestion"``, ``"processing"`` or
            ``"visualization"`` — selects which SASL credentials to use.

    Returns:
        A dictionary of confluent-kafka client configuration options including
        the bootstrap servers and SASL/PLAIN credentials for ``role``.

    Raises:
        KeyError: If ``role`` is not a known service role.
    """
    username, password = CREDENTIALS[role]
    return {
        "bootstrap.servers": get_bootstrap_servers() or DEFAULT_BOOTSTRAP,
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "PLAIN",
        "sasl.username": username,
        "sasl.password": password,
    }


def brokers_reachable(timeout: float = 5.0) -> bool:
    """Return True if the configured Kafka brokers respond to a metadata query.

    Uses an admin (super-user) SASL connection to list cluster metadata. Any
    connection, authentication, or timeout error is treated as "not reachable"
    so callers can skip gracefully.

    Args:
        timeout: Maximum time in seconds to wait for a metadata response.

    Returns:
        ``True`` if metadata was retrieved successfully, ``False`` otherwise.
    """
    if not CONFLUENT_KAFKA_AVAILABLE or get_bootstrap_servers() is None:
        return False

    # Imported lazily so this module stays import-safe without the dependency.
    from confluent_kafka.admin import AdminClient

    try:
        admin = AdminClient(sasl_config("admin"))
        admin.list_topics(timeout=timeout)
        return True
    except Exception:  # noqa: BLE001 - any failure means "not reachable"
        return False


def requires_kafka() -> None:
    """Skip the calling test unless a live, reachable Kafka cluster exists.

    Call this at the top of an integration test (or depend on the
    ``kafka_cluster`` fixture) to enforce the skip-guard chain:

    1. ``confluent-kafka`` must be importable.
    2. ``KAFKA_BOOTSTRAP_SERVERS`` must be set.
    3. The brokers must actually be reachable.
    """
    if not CONFLUENT_KAFKA_AVAILABLE:
        pytest.skip("confluent-kafka is not installed")
    if get_bootstrap_servers() is None:
        pytest.skip(
            "KAFKA_BOOTSTRAP_SERVERS is not set; skipping live integration test"
        )
    if not brokers_reachable():
        pytest.skip(
            "Kafka brokers are not reachable at "
            f"{get_bootstrap_servers()}; skipping live integration test"
        )


@pytest.fixture(scope="session")
def kafka_cluster() -> str:
    """Session fixture that yields the bootstrap servers or skips.

    Enforces the full skip-guard chain via :func:`requires_kafka` and then
    returns the bootstrap server string for tests to use.

    Returns:
        The Kafka bootstrap servers string.
    """
    requires_kafka()
    return get_bootstrap_servers()
