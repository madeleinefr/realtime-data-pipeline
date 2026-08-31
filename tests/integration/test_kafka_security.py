"""Integration tests for Kafka SASL authentication and ACL enforcement.

Verifies the application-layer security controls of the cluster against a
running Docker Compose stack:

    - Connections without valid SASL/PLAIN credentials are rejected.
    - Connections with valid credentials are accepted.
    - Per-service ACLs enforce least privilege: each service may only perform
      the operations granted to it (ingestion=WRITE raw_data,
      processing=READ raw_data + WRITE aggregated_data,
      visualization=READ aggregated_data). Unauthorized operations are
      rejected by the broker.

The module is import-safe and skips gracefully when confluent-kafka is missing,
``KAFKA_BOOTSTRAP_SERVERS`` is unset, or the brokers are unreachable.

Validated requirements:
    - FR-2.6: at-least-once delivery for authorized writes.
    - NFR-5.3: SASL/PLAIN authentication required; invalid connections rejected.
    - NFR-5.6: authentication failures are rejected.
    - NFR-5.7: Kafka ACLs enforce least-privilege per service.
"""

import json
import time
import uuid

import pytest

from tests.integration.conftest import (
    AGGREGATED_DATA_TOPIC,
    CONFLUENT_KAFKA_AVAILABLE,
    RAW_DATA_TOPIC,
    VISUALIZATION_GROUP,
    get_bootstrap_servers,
    requires_kafka,
    sasl_config,
)

pytestmark = pytest.mark.integration


def _try_produce(role_conf: dict, topic: str) -> "object":
    """Attempt to produce one message and return the delivery error (or None).

    Produces a single small message synchronously and waits for the delivery
    report so that broker-side authorization decisions surface as delivery
    errors rather than silent async failures.

    Args:
        role_conf: A confluent-kafka producer configuration dict.
        topic: The topic to attempt to produce to.

    Returns:
        The ``KafkaError`` reported for the delivery, or ``None`` on success.
    """
    from confluent_kafka import Producer

    result = {"err": None, "done": False}

    def _cb(err, _msg):
        result["err"] = err
        result["done"] = True

    producer = Producer(role_conf)
    producer.produce(
        topic,
        key=b"device-001",
        value=json.dumps({"probe": str(uuid.uuid4())}).encode("utf-8"),
        callback=_cb,
    )
    # Drive the delivery callback to completion.
    end = time.time() + 30
    while not result["done"] and time.time() < end:
        producer.poll(0.5)
    producer.flush(timeout=10)
    return result["err"]


class TestSaslAuthentication:
    """NFR-5.3 / NFR-5.6: SASL/PLAIN authentication acceptance and rejection."""

    def test_valid_credentials_accepted(self, kafka_cluster):
        """**Validates: Requirements NFR-5.3**

        A client presenting valid SASL/PLAIN credentials (admin super-user)
        can retrieve cluster metadata.
        """
        from confluent_kafka.admin import AdminClient

        admin = AdminClient(sasl_config("admin"))
        metadata = admin.list_topics(timeout=10)
        assert metadata is not None
        assert metadata.brokers, "expected at least one broker in metadata"

    def test_invalid_credentials_rejected(self, kafka_cluster):
        """**Validates: Requirements NFR-5.3, NFR-5.6**

        A client presenting invalid SASL/PLAIN credentials cannot retrieve
        cluster metadata; the connection is rejected (surfaced as an error or
        an empty/timed-out metadata call).
        """
        from confluent_kafka.admin import AdminClient

        bad_conf = sasl_config("admin")
        bad_conf["sasl.username"] = "admin"
        bad_conf["sasl.password"] = "definitely-wrong-" + uuid.uuid4().hex

        admin = AdminClient(bad_conf)
        rejected = False
        try:
            metadata = admin.list_topics(timeout=10)
            # If no exception is raised, a rejected auth surfaces as no
            # reachable brokers in the returned metadata.
            if not metadata.brokers:
                rejected = True
        except Exception:  # noqa: BLE001 - auth failure raises KafkaException
            rejected = True

        assert rejected, "connection with invalid credentials was not rejected"


class TestAclEnforcement:
    """NFR-5.7: per-service ACLs enforce least privilege."""

    def test_ingestion_can_write_raw_data(self, kafka_cluster):
        """**Validates: Requirements NFR-5.7, FR-2.6**

        The ingestion service (WRITE raw_data) can produce to raw_data.
        """
        err = _try_produce(sasl_config("ingestion"), RAW_DATA_TOPIC)
        assert err is None, f"ingestion should be allowed to write raw_data: {err}"

    def test_ingestion_cannot_write_aggregated_data(self, kafka_cluster):
        """**Validates: Requirements NFR-5.7**

        The ingestion service has no WRITE grant on aggregated_data, so the
        broker must reject the produce with an authorization error.
        """
        err = _try_produce(sasl_config("ingestion"), AGGREGATED_DATA_TOPIC)
        assert err is not None, (
            "ingestion must NOT be allowed to write aggregated_data"
        )

    def test_visualization_cannot_write_aggregated_data(self, kafka_cluster):
        """**Validates: Requirements NFR-5.7**

        The visualization service is read-only on aggregated_data; a produce
        attempt must be rejected by the broker.
        """
        err = _try_produce(sasl_config("visualization"), AGGREGATED_DATA_TOPIC)
        assert err is not None, (
            "visualization must NOT be allowed to write aggregated_data"
        )

    def test_visualization_cannot_read_raw_data(self, kafka_cluster):
        """**Validates: Requirements NFR-5.7**

        The visualization service has no READ grant on raw_data. Subscribing
        and polling must surface an authorization error rather than records.
        """
        from confluent_kafka import Consumer, KafkaError

        # Use the authorized consumer group for the visualization user
        # (``visualization-group``) so the block surfaces as a *topic* denial on
        # raw_data rather than a group denial on a randomized, unauthorized
        # group. Either outcome proves the read is blocked, so both
        # TOPIC_AUTHORIZATION_FAILED and GROUP_AUTHORIZATION_FAILED are accepted
        # below.
        conf = sasl_config("visualization")
        conf.update(
            {
                "group.id": VISUALIZATION_GROUP,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer = Consumer(conf)
        consumer.subscribe([RAW_DATA_TOPIC])

        authorization_error = False
        try:
            end = time.time() + 20
            while time.time() < end:
                msg = consumer.poll(timeout=2.0)
                if msg is None:
                    continue
                if msg.error():
                    code = msg.error().code()
                    if code in (
                        KafkaError.TOPIC_AUTHORIZATION_FAILED,
                        KafkaError.GROUP_AUTHORIZATION_FAILED,
                    ):
                        authorization_error = True
                        break
                else:
                    # Receiving an actual record means the ACL failed to block.
                    pytest.fail(
                        "visualization unexpectedly read a record from raw_data"
                    )
        finally:
            consumer.close()

        assert authorization_error, (
            "expected an authorization error reading raw_data as visualization"
        )


@pytest.mark.skipif(
    not CONFLUENT_KAFKA_AVAILABLE,
    reason="confluent-kafka is not installed",
)
def test_bootstrap_servers_documented_when_running():
    """Sanity check: when opted in, bootstrap servers are configured.

    This does not require a reachable broker (it only reads configuration), so
    it verifies the opt-in mechanism itself. It skips when the caller has not
    set ``KAFKA_BOOTSTRAP_SERVERS``.
    """
    if get_bootstrap_servers() is None:
        pytest.skip("KAFKA_BOOTSTRAP_SERVERS is not set")
    assert isinstance(get_bootstrap_servers(), str)
    assert get_bootstrap_servers(), "bootstrap servers must be non-empty"
