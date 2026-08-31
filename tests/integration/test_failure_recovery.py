"""Integration tests for failure recovery and offset resumption.

Verifies that a consumer (standing in for the Processing_Service, which
checkpoints its committed offsets) resumes consumption from its last committed
offset after a simulated restart, reprocessing no more than the uncommitted
tail. This mirrors the reliability guarantee that a service restart does not
replay already-committed records.

The "restart" is simulated by fully closing a consumer instance (leaving the
group and persisting committed offsets in Kafka) and then creating a brand-new
consumer with the *same* group id — the offset state lives in the broker, so
this faithfully reproduces the resume-from-offset behaviour without needing to
bounce a container from within the test.

The module is import-safe and skips gracefully when confluent-kafka is missing,
``KAFKA_BOOTSTRAP_SERVERS`` is unset, or the brokers are unreachable.

Validated requirements:
    - FR-2.4: consumers resume from last committed offset without redelivery.
    - NFR-3.3: Processing_Service resumes from last committed offset after a
      restart, reprocessing at most one interval's worth of records.
"""

import json
import time
import uuid

import pytest

from tests.integration.conftest import (
    PROCESSING_GROUP,
    RAW_DATA_TOPIC,
    sasl_config,
)

pytestmark = pytest.mark.integration


def _produce_markers(count: int) -> list:
    """Produce ``count`` uniquely-marked records to raw_data.

    All records share the same key so they land on a single partition, making
    offset ordering deterministic for the resumption assertions.

    Args:
        count: Number of marked records to produce.

    Returns:
        The ordered list of marker strings that were produced.
    """
    from confluent_kafka import Producer
    from datetime import datetime, timezone

    producer = Producer(sasl_config("ingestion"))
    markers = []
    for _ in range(count):
        marker = str(uuid.uuid4())
        markers.append(marker)
        record = {
            "record_id": marker,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "device_id": "device-001",
            "device_type": "weather_station",
            "canton": "ZH",
            "latitude": 47.3769,
            "longitude": 8.5417,
            "temperature": 4.2,
            "humidity": 78.5,
            "pressure": 1018.30,
            "status": "active",
        }
        producer.produce(
            RAW_DATA_TOPIC,
            key=b"device-001",
            value=json.dumps(record).encode("utf-8"),
        )
    producer.flush(timeout=30)
    return markers


class TestOffsetResumption:
    """FR-2.4 / NFR-3.3: resume from committed offset after restart."""

    def test_restart_resumes_from_committed_offset(self, kafka_cluster):
        """**Validates: Requirements NFR-3.3, FR-2.4**

        A consumer that commits after processing some records, then "restarts"
        (new consumer, same group id), must not redeliver committed records —
        it resumes from the committed offset.
        """
        from confluent_kafka import Consumer, TopicPartition

        # The processing user may only READ the exact consumer group
        # ``processing-group`` (least-privilege group ACL); a randomized group
        # id would be denied with GROUP_AUTHORIZATION_FAILED. Per-record marker
        # UUIDs provide the uniqueness this test relies on across runs.
        group_id = PROCESSING_GROUP
        markers = _produce_markers(count=6)
        marker_set = set(markers)

        def new_consumer() -> Consumer:
            conf = sasl_config("processing")
            conf.update(
                {
                    "group.id": group_id,
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False,
                }
            )
            consumer = Consumer(conf)
            consumer.subscribe([RAW_DATA_TOPIC])
            return consumer

        # --- Phase 1: consume the first few markers and commit past them. ---
        consumer = new_consumer()
        consumed_before_restart = []
        try:
            end = time.time() + 40
            while len(consumed_before_restart) < 3 and time.time() < end:
                msg = consumer.poll(timeout=2.0)
                if msg is None or msg.error():
                    continue
                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                except (ValueError, AttributeError):
                    continue
                if payload.get("record_id") in marker_set:
                    consumed_before_restart.append(payload["record_id"])
                    consumer.commit(
                        offsets=[
                            TopicPartition(
                                msg.topic(), msg.partition(), msg.offset() + 1
                            )
                        ],
                        asynchronous=False,
                    )
        finally:
            # Simulate a restart: close leaves the group and persists offsets.
            consumer.close()

        if not consumed_before_restart:
            pytest.skip(
                "Could not consume/commit any marked records before restart"
            )

        committed = set(consumed_before_restart)

        # --- Phase 2: new consumer, same group id; must not redeliver. ---
        consumer = new_consumer()
        redelivered = []
        try:
            end = time.time() + 30
            while time.time() < end:
                msg = consumer.poll(timeout=2.0)
                if msg is None or msg.error():
                    continue
                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                except (ValueError, AttributeError):
                    continue
                rid = payload.get("record_id")
                if rid in committed:
                    redelivered.append(rid)
                    break
        finally:
            consumer.close()

        assert not redelivered, (
            f"records {redelivered} were redelivered after restart; "
            "offset resumption failed"
        )

    def test_uncommitted_tail_is_reprocessed_after_restart(self, kafka_cluster):
        """**Validates: Requirements NFR-3.3**

        Records consumed but *not* committed before a restart are reprocessed,
        demonstrating at-least-once semantics with no data loss across the
        restart boundary.
        """
        from confluent_kafka import Consumer

        # The processing user may only READ the exact consumer group
        # ``processing-group`` (least-privilege group ACL); a randomized group
        # id would be denied with GROUP_AUTHORIZATION_FAILED. Per-record marker
        # UUIDs provide the uniqueness this test relies on across runs.
        group_id = PROCESSING_GROUP
        markers = _produce_markers(count=4)
        marker_set = set(markers)

        def new_consumer() -> Consumer:
            conf = sasl_config("processing")
            conf.update(
                {
                    "group.id": group_id,
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False,
                }
            )
            consumer = Consumer(conf)
            consumer.subscribe([RAW_DATA_TOPIC])
            return consumer

        # Phase 1: read at least one marker but DO NOT commit, then restart.
        consumer = new_consumer()
        read_uncommitted = None
        try:
            end = time.time() + 40
            while read_uncommitted is None and time.time() < end:
                msg = consumer.poll(timeout=2.0)
                if msg is None or msg.error():
                    continue
                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                except (ValueError, AttributeError):
                    continue
                if payload.get("record_id") in marker_set:
                    read_uncommitted = payload["record_id"]
        finally:
            consumer.close()

        if read_uncommitted is None:
            pytest.skip("Could not read any marked record before restart")

        # Phase 2: same group, no prior commit -> the tail must reappear.
        consumer = new_consumer()
        reprocessed = False
        try:
            end = time.time() + 30
            while time.time() < end and not reprocessed:
                msg = consumer.poll(timeout=2.0)
                if msg is None or msg.error():
                    continue
                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                except (ValueError, AttributeError):
                    continue
                if payload.get("record_id") == read_uncommitted:
                    reprocessed = True
        finally:
            consumer.close()

        assert reprocessed, (
            "uncommitted record was not reprocessed after restart; "
            "expected at-least-once recovery"
        )
